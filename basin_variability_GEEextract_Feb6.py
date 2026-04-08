#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created in 2024 by ericlevenson, updated by E. Webb in Jan 2026
"""
import ee
import logging
import multiprocessing
from retry import retry
import sys
import pandas as pd
import time
from datetime import datetime
import gc
import os

logging.getLogger('googleapiclient').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

ee.Initialize(project="focal-psyche-100516")
ee.Authenticate()

SINGLE_BASIN_ID = None ##"3315024" #None  # e.g. 
DEBUG_MODE = False
DEBUG_BASIN_ID = "3315024"
DEBUG_DATE = "2017-06-28"

basinlevel = 7
exportSelectors = ['date', 'lake_id', 'waterArea_m2', 'lake_cloud_coverage']
CLD_PRB_THRESH = 50

performance_log_path = '/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Basin_variability/data_curation/basin_variability_performance_log.csv'
lakes_within_basin_lookup_path = '/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Basin_variability/data_curation/basin_lake_match_PFAF.csv'
basins_to_sample_path = '/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Basin_variability/data_curation/basins_to_process_1000_sample4.csv'
valid_dates_input_dir = '/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Basin_variability/data_curation/valid_dates_output'
output_dir = '/Users/elizabethwebb/Library/CloudStorage/GoogleDrive-webb.elizabeth.e@gmail.com/My Drive/PostDoc/Basin_variability/raw_GEE_output_per_date'

# GEE assets
lake_sample = ee.FeatureCollection('projects/focal-psyche-100516/assets/PLD_random_20percent_fixed')
lake_sample = lake_sample.merge(ee.FeatureCollection('projects/focal-psyche-100516/assets/PLD_random_20-40percent_fixed'))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_random_40-60percent_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_random_60-80percent_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_random_80pluspercent_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff_0_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__1_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__2a_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__2b_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__3_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__4_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__5_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__6a_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__6b_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__7_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__8_fixed"))
lake_sample = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__9_fixed"))
lakes = lake_sample.merge(ee.FeatureCollection("projects/focal-psyche-100516/assets/PLD_south_buff__10_fixed"))

basin_fc = ee.FeatureCollection('WWF/HydroSHEDS/v1/Basins/hybas_7')

# Load lookup
try:
    basin_lake_lookup_df = pd.read_csv(lakes_within_basin_lookup_path, dtype={"PFAF_ID": str, "lake_id": str})
    basin_lake_lookup_df = basin_lake_lookup_df[basin_lake_lookup_df["level"].astype(str) == str(basinlevel)]
    basin_to_lakeids = (basin_lake_lookup_df.groupby("PFAF_ID")["lake_id"].apply(list).to_dict())
    print(f"Loaded basin->lake_id lookup for {len(basin_to_lakeids)} basins")
except Exception as e:
    print(f"Warning: Could not load basin->lake_id lookup: {e}")
    basin_lake_lookup_df = pd.DataFrame(columns=["region", "level", "PFAF_ID", "lake_id"])
    basin_to_lakeids = {}

# basins list
try:
    if SINGLE_BASIN_ID is not None:
        ids = [str(SINGLE_BASIN_ID)]
        print(f"Single-basin mode: running basin {SINGLE_BASIN_ID}")
    else:
        basins_to_sample_df = pd.read_csv(basins_to_sample_path, dtype={"PFAF_ID": str})
        ids = basins_to_sample_df["PFAF_ID"].dropna().astype(str).tolist()
        print(f"Loaded {len(ids)} basin IDs to process")
except Exception as e:
    raise RuntimeError(f"Failed to load basins_to_sample CSV: {e}")

lakes = lakes.map(lambda f: f.set('lake_id_str', ee.String(f.get('lake_id'))))
os.makedirs(output_dir, exist_ok=True)

def get_valid_dates_for_basin(basinID):
    valid_dates_csv = os.path.join(valid_dates_input_dir, f"{basinID}_valid_dates.csv")
    if not os.path.exists(valid_dates_csv):
        print(f"Warning: No valid dates CSV found for basin {basinID} at {valid_dates_csv}")
        return None
    try:
        dates_df = pd.read_csv(valid_dates_csv)
        if 'valid_date' not in dates_df.columns:
            print(f"Warning: valid_date column not found in {valid_dates_csv}")
            return None
        valid_dates = dates_df['valid_date'].dropna().tolist()
        print(f"Basin {basinID}: Loaded {len(valid_dates)} valid dates from CSV")
        return valid_dates
    except Exception as e:
        print(f"Error reading valid dates CSV for basin {basinID}: {e}")
        return None

def log_performance(basin_id, processing_time, num_dates_attempted,
                   status="success", error_msg="",
                   num_lakes=None, num_skipped=None):
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'PFAF_ID': basin_id,
        'processing_time_sec': round(processing_time, 2),
        'num_dates_attempted': num_dates_attempted,
        'dates_per_second': round(num_dates_attempted / processing_time, 3) if processing_time > 0 else 0,
        'status': status,
        'error_msg': error_msg,
        'num_lakes': num_lakes,
        'num_dates_skipped': num_skipped
    }
    file_exists = os.path.exists(performance_log_path)
    try:
        import csv
        if file_exists:
            try:
                perf_df = pd.read_csv(performance_log_path, dtype={"PFAF_ID": str})
                perf_df = perf_df[perf_df["PFAF_ID"].astype(str) != str(basin_id)]
                new_entry_df = pd.DataFrame([log_entry])
                perf_df = pd.concat([perf_df, new_entry_df], ignore_index=True)
                perf_df.to_csv(performance_log_path, index=False)
                return
            except Exception:
                pass
        with open(performance_log_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=log_entry.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(log_entry)
    except Exception as e:
        print(f"Warning: Could not write performance log: {e}")

@retry(tries=2, delay=5, backoff=2)
def getResult(index, basinID):
    try:
        ee.Initialize(project="focal-psyche-100516", opt_url='https://earthengine-highvolume.googleapis.com')
    except Exception:
        pass

    start_time = time.time()
    num_images_processed = 0
    num_dates_attempted = 0
    num_lakes = None
    num_skipped = 0
    gc.collect()

    try:
        valid_dates = get_valid_dates_for_basin(basinID)
        if valid_dates is None or len(valid_dates) == 0:
            print(f"Skipping basin {basinID}: no valid dates available", flush=True)
            return

        if DEBUG_MODE and str(basinID) == str(DEBUG_BASIN_ID):
            if DEBUG_DATE in valid_dates:
                valid_dates = [DEBUG_DATE]
            else:
                print(f"WARNING: Debug date {DEBUG_DATE} not in valid dates!", flush=True)
                return

        num_dates_attempted = len(valid_dates)

        if SINGLE_BASIN_ID is None:
            try:
                dates_to_process = []
                existing_count = 0
                for date in valid_dates:
                    csv_path = os.path.join(output_dir, f"{basinID}_{date}.csv")
                    if os.path.exists(csv_path):
                        existing_count += 1
                    else:
                        dates_to_process.append(date)
                if len(dates_to_process) == 0:
                    print(f"Skipping basin {basinID}: all {len(valid_dates)} CSV files already exist.", flush=True)
                    return
                elif existing_count > 0:
                    valid_dates = dates_to_process
                    num_dates_attempted = len(valid_dates)
            except Exception:
                pass

        # ---- GEE functions ----
        def add_cloud_bands(img):
            cld_prb = ee.Image(img.get('s2cloudless')).select('probability')
            clouds = cld_prb.gte(CLD_PRB_THRESH).rename('cloud_mask')
            clear = cld_prb.lt(CLD_PRB_THRESH).rename('clear_mask')
            return img.addBands(ee.Image([cld_prb, clouds, clear]))

        def clip_image(image):
            return image.clip(roi)

        def clip2lakes(image):
            return image.clip(lakes_in_basin)

        def mosaicBy(imcol):
            imlist = imcol.toList(imcol.size())
            def imdate(im):
                date = ee.Image(im).date().format("YYYY-MM-dd")
                return date
            all_dates = imlist.map(imdate)
            def orbitId(im):
                orb = ee.Image(im).get('SENSING_ORBIT_NUMBER')
                return orb
            all_orbits = imlist.map(orbitId)
            def spacecraft(im):
                return ee.Image(im).get('SPACECRAFT_NAME')
            all_spNames = imlist.map(spacecraft)
            concat_all = all_dates.zip(all_orbits).zip(all_spNames)
            def concat(el):
                return ee.List(el).flatten().join(" ")
            concat_all = concat_all.map(concat)
            concat_unique = concat_all.distinct()
            def mosaicIms(d):
                d1 = ee.String(d).split(" ")
                date1 = ee.Date(d1.get(0))
                orbit = ee.Number.parse(d1.get(1)).toInt()
                spName = ee.String(d1.get(2))
                im = imcol.filterDate(date1, date1.advance(1, "day")) \
                          .filterMetadata('SPACECRAFT_NAME', 'equals', spName) \
                          .filterMetadata('SENSING_ORBIT_NUMBER','equals', orbit).mosaic()
                return im.set("system:time_start", date1.millis(),
                              "system:date", date1.format("YYYY-MM-dd"),
                              "system:id", d1)
            mosaic_imlist = concat_unique.map(mosaicIms)
            return ee.ImageCollection(mosaic_imlist)

        def ndwi(image):
            return image.normalizedDifference(['B3', 'B8']).rename('NDWI').multiply(1000)

        def otsu(histogram):
            counts = ee.Array(ee.Dictionary(histogram).get('histogram'))
            means = ee.Array(ee.Dictionary(histogram).get('bucketMeans'))
            size = means.length().get([0])
            total = counts.reduce(ee.Reducer.sum(), [0]).get([0])
            sum = means.multiply(counts).reduce(ee.Reducer.sum(), [0]).get([0])
            mean = sum.divide(total)
            indices = ee.List.sequence(1, size)
            def func_xxx(i):
                aCounts = counts.slice(0, 0, i)
                aCount = aCounts.reduce(ee.Reducer.sum(), [0]).get([0])
                aMeans = means.slice(0, 0, i)
                aMean = aMeans.multiply(aCounts).reduce(ee.Reducer.sum(), [0]).get([0]).divide(aCount)
                bCount = total.subtract(aCount)
                bMean = sum.subtract(aCount.multiply(aMean)).divide(bCount)
                return aCount.multiply(aMean.subtract(mean).pow(2)).add(bCount.multiply(bMean.subtract(mean).pow(2)))
            bss = indices.map(func_xxx)
            return means.sort(bss).get([-1])

        def otsu_thresh(water_image):
            NDWI = ndwi(water_image).select('NDWI').updateMask(water_image.select('clear_mask'))
            histogram = ee.Dictionary(NDWI.reduceRegion(
                geometry=roi,
                reducer=ee.Reducer.histogram(255, 2).combine('mean', None, True).combine('variance', None, True),
                scale=50,
                maxPixels=1e12
            ))
            has_hist = histogram.contains('NDWI_histogram')
            return ee.Algorithms.If(has_hist, otsu(histogram.get('NDWI_histogram')), ee.Number(-9999))

        def adaptive_thresholding(water_image):
            NDWI = ndwi(water_image).select('NDWI').updateMask(water_image.select('clear_mask'))
            threshold_num = ee.Number(otsu_thresh(water_image))
            def compute_with_threshold():
                thr = threshold_num.divide(10).round().multiply(10)
                histo = NDWI.reduceRegion(
                    geometry=roi,
                    reducer=ee.Reducer.fixedHistogram(-1000, 1000, 200),
                    scale=50,
                    maxPixels=1e12
                )
                hist = ee.Array(histo.get('NDWI'))
                cond = ee.Algorithms.IsEqual(hist, None)
                def _adaptive_from_hist():
                    counts = hist.cut([-1,1])
                    buckets = hist.cut([-1,0])
                    threshold_arr = ee.Array([thr]).toList()
                    buckets_list = buckets.toList()
                    split = buckets_list.indexOf(threshold_arr)
                    split = ee.Number(split).max(1)
                    land_slice = counts.slice(0,0,split)
                    water_slice = counts.slice(0,split.add(1),-1)
                    land_max = land_slice.reduce(ee.Reducer.max(),[0]).toList().get(0)
                    water_max = water_slice.reduce(ee.Reducer.max(),[0]).toList().get(0)
                    land_max = ee.List(land_max).getNumber(0)
                    water_max = ee.List(water_max).getNumber(0)
                    counts_list = counts.toList()
                    # FIX: ensure scalar extraction from possibly-list element
                    otsu_val = ee.List(counts_list.get(split)).getNumber(0)
                    # ensure ee.Number usage for subsequent arithmetic
                    otsu_val = ee.Number(otsu_val)
                    land_prom = ee.Number(land_max).subtract(otsu_val)
                    water_prom = ee.Number(water_max).subtract(otsu_val)
                    land_thresh = ee.Number(land_max).subtract((land_prom).multiply(ee.Number(0.9)))
                    water_thresh = ee.Number(water_max).subtract((water_prom).multiply(ee.Number(0.9)))
                    land_max_ind = land_slice.argmax().get(0)
                    water_max_ind = water_slice.argmax().get(0)
                    li = ee.Number(land_max_ind).subtract(1).max(ee.Number(1))
                    wi = ee.Number(water_max_ind).add(1).min(ee.Number(199))
                    land_slice2 = land_slice.slice(0,li,-1).subtract(land_thresh)
                    water_slice2 = water_slice.slice(0,0,wi).subtract(water_thresh)
                    land_slice2  = land_slice2.abs().multiply(-1)
                    water_slice2 = water_slice2.abs().multiply(-1)
                    land_index = ee.Number(land_slice2.argmax().get(0)).add(land_max_ind)
                    water_index = ee.Number(water_slice2.argmax().get(0)).add(split)
                    land_level = ee.Number(buckets_list.get(land_index))
                    water_level = ee.Number(buckets_list.get(water_index))
                    land_level = ee.Number(ee.List(land_level).get(0)).add(5)
                    water_level = ee.Number(ee.List(water_level).get(0)).add(5)
                    water_fraction = (NDWI.subtract(land_level)).divide(water_level.subtract(land_level)).multiply(100).rename('water_fraction')
                    water_25 = water_fraction.gte(25).rename('water_25')
                    return water_image.addBands([water_25])
                return ee.Algorithms.If(cond, water_image.addBands(ee.Image(0).rename('water_25')), _adaptive_from_hist())
            return ee.Image(ee.Algorithms.If(threshold_num.gt(-9000), compute_with_threshold(), water_image.addBands(ee.Image(0).rename('water_25'))))

        def pixelArea(image):
            areaIm = image.pixelArea()
            return image.addBands([areaIm])

        def perLakeProps(image):
            date = image.date().format('yyyy-MM-dd')
            water25_area = image.select('area').updateMask(image.select('water_25')).rename('water25_area')
            cloud_mask = image.select('cloud_mask')
            stats_img = water25_area.addBands(cloud_mask)
            reducer = ee.Reducer.sum().combine(reducer2=ee.Reducer.max(), sharedInputs=True)
            stats_fc = stats_img.reduceRegions(collection=lakes_in_basin, reducer=reducer, scale=10, tileScale=4)
            def feat_to_feature(f):
                f = ee.Feature(f)
                lake_id = f.get('lake_id')
                water_m2 = ee.Number(f.get('water25_area_sum'))
                water_m2 = ee.Number(ee.Algorithms.If(water_m2, water_m2, 0))
                cloud_any = ee.Number(f.get('cloud_mask_max'))
                cloud_any = ee.Number(ee.Algorithms.If(cloud_any, cloud_any, 0)).toInt()
                props = {
                    'date': date,
                    'lake_id': lake_id,
                    'waterArea_m2': water_m2,
                    'lake_cloud_coverage': cloud_any,
                }
                return ee.Feature(None, props)
            result_fc = ee.FeatureCollection(stats_fc.map(feat_to_feature)).filter(ee.Filter.notNull(['lake_id']))
            return result_fc

        # basin geometry
        basin = ee.Feature(basin_fc.filter(ee.Filter.eq('PFAF_ID', int(basinID))).first())
        basin_dict = basin.getInfo() if basin else None
        if not basin_dict or 'geometry' not in basin_dict:
            print(f"No basin geometry found for PFAF_ID {basinID}; skipping", flush=True)
            return
        roi = ee.Geometry.MultiPolygon(basin.geometry().getInfo()['coordinates'][:])

        lake_ids = basin_to_lakeids.get(str(basinID), [])
        num_lakes = len(lake_ids)
        if num_lakes == 0:
            print(f"Skipping basin {basinID}: no lake_ids in lookup", flush=True)
            return
        lake_ids_int = [int(x) for x in lake_ids]
        lakes_in_basin = lakes.filter(ee.Filter.inList('lake_id', ee.List(lake_ids_int)))
        lake_count = lakes_in_basin.size().getInfo()
        print(f"Basin {basinID}: {lake_count} lakes after filtering", flush=True)

        try:
            date_filters = [ee.Filter.date(d, ee.Date(d).advance(1, 'day')) for d in valid_dates]
            date_filter = ee.Filter.Or(*date_filters)
            images = ee.ImageCollection('COPERNICUS/S2_HARMONIZED').filterBounds(roi).filter(date_filter)
            s2Cloudless = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY').filterBounds(roi).filter(date_filter)
            images = ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(**{
                'primary': images,
                'secondary': s2Cloudless,
                'condition': ee.Filter.equals(**{'leftField': 'system:index', 'rightField': 'system:index'})
            }))
            images = images.map(add_cloud_bands)
            images_all = mosaicBy(images)
            images_all = images_all.map(clip_image)
            lakeimages = images_all.map(clip2lakes)
            lakeimages = lakeimages.map(adaptive_thresholding)
            lakeimages = lakeimages.map(pixelArea)

            # attach per-image outputs with fixed upper bound
            lakeimages = lakeimages.map(lambda im: im.set('output', perLakeProps(im).toList(10000)))

            result_nested = lakeimages.aggregate_array('output').getInfo()

            written_files = []
            skipped_dates = []

            for date_result in result_nested:
                try:
                    if date_result is None:
                        skipped_dates.append(("unknown_date", "no_features"))
                        continue
                    if isinstance(date_result, list):
                        features = date_result
                    elif isinstance(date_result, dict) and 'features' in date_result:
                        features = date_result['features']
                    else:
                        skipped_dates.append(("unknown_date", "unexpected_result_shape"))
                        continue
                    if not features:
                        skipped_dates.append(("unknown_date", "empty_features"))
                        continue
                    first = features[0]
                    if isinstance(first, dict) and 'properties' in first:
                        rows = [f.get('properties', {}) for f in features]
                    elif isinstance(first, dict):
                        rows = [f for f in features]
                    else:
                        skipped_dates.append(("unknown_date", "unexpected_feature_type"))
                        continue
                    date_str = rows[0].get('date', 'unknown_date')
                    df = pd.DataFrame(rows)
                    csv_filename = f"{basinID}_{date_str}.csv"
                    csv_path = os.path.join(output_dir, csv_filename)
                    df.to_csv(csv_path, index=False)
                    written_files.append(csv_filename)
                except Exception as e:
                    error_date = "unknown_date"
                    try:
                        if 'rows' in locals() and len(rows) > 0:
                            error_date = rows[0].get('date','unknown_date')
                    except Exception:
                        pass
                    skipped_dates.append((error_date, f"error:{str(e)}"))
                    continue

            num_images_processed = len(written_files)
            num_skipped = len(skipped_dates)

            processing_time = time.time() - start_time
            log_performance(basinID, processing_time, num_dates_attempted, "success", "", num_lakes, num_skipped)
            print(f"Done: {index} | Basin: {basinID} | Time: {processing_time:.1f}s | Files written: {num_images_processed} | Skipped: {num_skipped}", flush=True)

        except ee.ee_exception.EEException as e:
            msg = str(e).lower()
            if "memory limit exceeded" in msg or "computation timed out" in msg:
                drive_folder = 'raw_GEE_output_per_date'
                date_filters = [ee.Filter.date(d, ee.Date(d).advance(1, 'day')) for d in valid_dates]
                date_filter = ee.Filter.Or(*date_filters)
                images = ee.ImageCollection('COPERNICUS/S2_HARMONIZED').filterBounds(roi).filter(date_filter)
                s2Cloudless = ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY').filterBounds(roi).filter(date_filter)
                images = ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(**{
                    'primary': images,
                    'secondary': s2Cloudless,
                    'condition': ee.Filter.equals(**{'leftField': 'system:index', 'rightField': 'system:index'})
                }))
                images = images.map(add_cloud_bands)
                images_all = mosaicBy(images)
                images_all = images_all.map(clip_image)
                lakeimages = images_all.map(clip2lakes)
                lakeimages = lakeimages.map(adaptive_thresholding)
                lakeimages = lakeimages.map(pixelArea)
                started_tasks = []
                for valid_date in valid_dates:
                    try:
                        date_filter_single = ee.Filter.date(valid_date, ee.Date(valid_date).advance(1, 'day'))
                        date_images = lakeimages.filter(date_filter_single)
                        date_features = date_images.map(perLakeProps).flatten()
                        file_prefix = f"{basinID}_{valid_date}"
                        task = ee.batch.Export.table.toDrive(
                            collection=date_features,
                            description=f"{basinID}_{valid_date}",
                            folder=drive_folder,
                            fileNamePrefix=file_prefix,
                            fileFormat='CSV',
                            selectors=exportSelectors
                        )
                        task.start()
                        started_tasks.append((file_prefix, valid_date))
                    except Exception:
                        continue
                processing_time = time.time() - start_time
                log_performance(basinID, processing_time, num_dates_attempted,
                                "fallback_export",
                                f"Memory limit exceeded, fallback exported {len(started_tasks)} dates to Drive folder: {drive_folder}",
                                num_lakes, num_dates_attempted - len(started_tasks))
                print(f"Done: {index} | Basin: {basinID} | Time: {processing_time:.1f}s | Fallback started {len(started_tasks)} exports", flush=True)
                return
            else:
                raise

    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = str(e)
        log_performance(basinID, processing_time, num_dates_attempted, "error", error_msg, num_lakes, num_skipped)
        print(f"Error processing basin {basinID}: {error_msg}", flush=True)
        return

if __name__ == '__main__':
    class FlushStreamHandler(logging.StreamHandler):
        def emit(self, record):
            super().emit(record)
            self.flush()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[FlushStreamHandler(sys.stderr)])
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    if not ids:
        raise RuntimeError("No basin IDs to process (check basins_to_sample_path).")

    if SINGLE_BASIN_ID is not None:
        getResult(0, ids[0])
    else:
        pool = multiprocessing.Pool(10)
        try:
            pool.starmap(getResult, enumerate(ids))
        finally:
            pool.close()
            pool.join()
