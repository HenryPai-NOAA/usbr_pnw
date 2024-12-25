#!/awips2/python/bin/python

# edits by:     henry pai (nwrfc)
# contact:      henry <dot> pai <at> noaa <dot> gov
# last edit:    hp, dec 2024
# edit notes:   usbr scraper rewrite

# notes: rewrite of usbr script, control files (alias, sites, post_control) changed to csv's, but post control 
#   left out exclusions, time zones (already in control), and initial lines not associated with id mapping.
# general logic: multline call with all usbr_ids (under 1024 chars) but excluding usbr pe's -> inner joins 
#   matching alias pes ->  inner join with id & pe code pairs/sites file -> inner join with ids/post_control   
# usage: 
# - default args:   python get_usbr_shef.py --locid all --duration realtime --back 3
# - daily call:     python get_usbr_shef.py --duration daily  <- minimalist, given default args are all and 3 
# - station call:   python get_usbr_shef.py --locid LUCI1 --duration realtime --back 24

import os, argparse, requests, pathlib, urllib, pdb, logging, shutil, yaml
from functools import reduce
from datetime import datetime, timezone
from io import StringIO
import lxml.html as lh
import pandas as pd
import numpy as np

os.umask(0o002)

# ===== global var (not path related)
shef_header = True
out_fmt = "shef"  # csv or shef
type_source = 'RZ'
product_id = 'usbrWEB'
#max_call_num = 95 # under 1000 chars, for combined usbr id + pe
max_call_num = 400 # under 1000 chars, for just usbr ids

# from shef manual :https://www.weather.gov/media/mdl/SHEF_CodeManual_5July2012.pdf
# table 1, pg 4-5 (pg 75-76 in pdf)
div1000_pes = ["LS","QR","QI","QD","QT","QU","QP"] 

# ===== url info
# urls:
# https://www.usbr.gov/pn-bin/instant.pl?list=
# https://www.usbr.gov/pn-bin/daily.pl?list= 
base_url = 'https://www.usbr.gov/pn-bin/'
daily_url_suffix = 'daily.pl?'
instant_url_suffix = 'instant.pl?'

# ===== directories & filenames
if os.name == 'nt':
    work_dir = pathlib.Path(__file__).parent # IDE independent
    meta_dir = os.path.join(work_dir, "meta")
    out_dir = os.path.join(work_dir, "incoming")
    log_dir = os.path.join(work_dir, "logs")
else:
    work_dir = pathlib.Path("/data/ldad/snotel/")
    meta_dir = work_dir
    out_dir = pathlib.Path("/data/Incoming/")
    log_dir = pathlib.Path("/data/ldad/logs/")

#post_control_fn = 'post_control_ids.csv'
post_control_fn = 'post_control.c5'
realtime_alias_fn = 'usbr_realtime_alias.csv'
realtime_site_obs_fn = 'usbr_realtime_list.csv'
daily_alias_fn = 'usbr_daily_alias.csv'
daily_site_obs_fn = 'usbr_daily_list.csv'
yaml_fn = 'config.yaml'

log_fn = "usbr_scrape.log"
out_fn_pre = "usbr_scraped_"
new_fn_pre = "new_usbr_"
last_fn_pre = "last_usbr_"

with open(os.path.join(meta_dir, yaml_fn)) as f:
    yaml_data = yaml.full_load(f)
    request_headers = {'User-Agent' : yaml_data['user_agent']}

# ===== initial set up for requests and logging
logging.basicConfig(format='%(asctime)s %(levelname)-4s %(message)s',
                    filename=os.path.join(log_dir, log_fn),
                    filemode='w',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# ===== functions
def parse_args():
    """
    Sets default arguments, just for hour look back
    Default look back is three hours
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--locid',
        #default='LUCI1', # testing, has multiple partner ids
        #default = 'KADW1', # testing
        default='all',
        help="Location id (all/<LID of single station>)"
        )
    parser.add_argument(
        '--duration',
        #default='daily',
        default='realtime',
        help="Set interval (realtime/daily)"
        )
    parser.add_argument(
        '--back',
        default=3, # 3 hours or days
        help="Interval back in units of duration <int>"
        )

    return parser.parse_args()

def get_data(call_str, dur_id, back):
    '''
    generates call url, potentially with multiple stations and returns df without flags
    '''
    request_params = {'list' : ','.join(call_str),
                      'back' : str(back)}

    if dur_id == 'I':
        dur_url = base_url + instant_url_suffix
    elif dur_id == 'D':
        dur_url = base_url + daily_url_suffix

    usbr_url = dur_url + urllib.parse.urlencode(request_params)
    logging.info(usbr_url)
    response = requests.get(usbr_url, headers=request_headers)
    html_elements = lh.fragments_fromstring(response.text)
    table_text = html_elements[len(html_elements) - 1].text_content() # table is usually last element
    data_df = pd.read_csv(StringIO(table_text), skiprows=2, skipfooter=2, engine='python', dtype='str')
    data_df.rename(columns={data_df.columns[0] : 'DateTime'}, inplace=True)
    return_df = data_df
    return(return_df)

def org_data(data_df, tz, alias_df, map_df, sites_df, dur_id):
    '''
    organizing steps: melts (make more rows), removes NaN vals, divides 1000 for relevant PE's 
    '''
    long_df = pd.melt(data_df, id_vars='DateTime')
    long_df[['usbr_id', 'USBR_PE']] = long_df['variable'].str.strip().str.split(n=1, expand=True) # n=1 is needed, odd but works

    if dur_id == 'I':
        long_df['value'] = pd.to_numeric(long_df['value'].str[:-1].str.strip()) # last char is a flag, and several leading white spaces
    elif dur_id == 'D':
        long_df['value'] = long_df['value'].str.strip()
        long_df.loc[long_df['value'] == 'NO RECORD', 'value'] = np.nan
        long_df['value'] = pd.to_numeric(long_df['value'])

    long_df.loc[long_df['value'] == 998877, 'value'] = np.nan # common error, has flag n, but hard to find flag definitions

    # cleaned alias file to not have repetitive usbr pe/sensor codes causing lengths to differ after merge
    # removes row if post_control doesn't have
    site_sensor_str = sites_df['usbr_id'] + ' ' + sites_df['USBR_PE']
    
    map_pe_df = (long_df.dropna(subset=['value']).reset_index(drop=True)
                 .merge(alias_df[['USBR_PE', 'SHEF_PE']], how='inner', on='USBR_PE')) 
    map_id_df = (map_pe_df[(map_pe_df['usbr_id'] + ' ' + map_pe_df['USBR_PE']).str.strip().isin(site_sensor_str)]
                 .merge(map_df, how='inner', left_on='usbr_id', right_on='partner_id')
                 .drop(['variable', 'partner_id'], axis=1))

    unmapped_usbr_ids = map_pe_df.usbr_id[~map_pe_df.usbr_id.isin(map_id_df.usbr_id)].unique().tolist()
    
    # correct units according to units
    map_id_df.loc[map_id_df['SHEF_PE'].isin(div1000_pes), 'value'] = (
        map_id_df.loc[map_id_df['SHEF_PE'].isin(div1000_pes), 'value'] / 1000 ).round(3)
    
    map_id_df['tz'] = tz
    map_id_df['dur'] = dur_id

    return_df = map_id_df.copy()[map_id_df['nws_id'] != 'BORXX']
    return(unmapped_usbr_ids, return_df)

def write_header(utc_now, out_fullfn, out_fmt, header=None):
    """
    adds two line header for shef:
    TTAA00 KPTR <ddhhmm>
    usbrWEB

    just csv row header for csv
    """
    f = open(out_fullfn, 'w')
    
    if out_fmt == 'shef':
        f.write("TTAA00 KPTR " + utc_now.strftime("%d%H%M") + "\n")
        f.write(product_id + "\n")  # product_id
    elif out_fmt == 'csv':
        header_str = ','.join(header) + '\n'
        f.write(header_str)
        
    f.flush()
    f.close()

def write_new_lines(last_fullfn, new_fullfn, out_fullfn, out_fmt):
    """
    compare last file download and current file download, write only new lines
    """
    # skipping header rows
    if out_fmt == 'csv':
        start_row = 1
    elif out_fmt == 'shef':
        if shef_header:
            start_row = 2
        else:
            start_row = 0
    
    # save differences
    with open(new_fullfn, 'r') as newfile:
        new_lines = newfile.readlines()[start_row:]
        with open(last_fullfn, 'r') as lastfile:
            last_lines = lastfile.readlines()[start_row:]
            
            set_last = set(last_lines)
            diff = [x for x in new_lines if x not in set_last]
    
    # write new data to file or delete header file
    if len(diff) > 1:
        with open(out_fullfn, 'a') as file_out:
            for line in diff:
                file_out.write(line)
        new_data = True
    else:
        logging.info("no new lines of data observed")
        #os.remove(out_fullfn)
        new_data = False
    
    return(new_data)

def remove_dup_lines(out_fullfn):
    """
    removes duplicate lines: https://stackoverflow.com/questions/1215208/how-might-i-remove-duplicate-lines-from-a-file
    """
    shutil.copyfile(out_fullfn, out_fullfn + ".tmp")

    lines_seen = set() # holds lines already seen
    outfile = open(out_fullfn, "w")
    for line in open(out_fullfn + ".tmp", "r"):
        if line not in lines_seen: # not a duplicate
            outfile.write(line)
            lines_seen.add(line)
    outfile.close()

    os.remove(out_fullfn + ".tmp")

def write_output(df, dur_str, utc_now):
    '''
    writing output, preventing sending old data back to ihfs
    '''
    fn_time_str = utc_now.strftime('%Y%m%d_%H%M%S')
    out_fn = out_fn_pre + dur_str + "." + fn_time_str + "." + out_fmt

    new_fn = new_fn_pre + dur_str + "." + out_fmt
    last_fn = last_fn_pre + dur_str + "." + out_fmt

    new_fullfn = os.path.join(log_dir, new_fn)
    last_fullfn = os.path.join(log_dir, last_fn)
    final_out_fullfn = os.path.join(out_dir, out_fn)

    if out_fmt =='csv':
        df.to_csv(new_fullfn, index=False)
        csv_headers = df.columns
    else:
        # ZZ is extremum (none) and probability (none)
        # DUE - E part means egnlish units
        if dur_str == 'realtime':
            out_lines = (".AR " + df.nws_id + " " 
                        + pd.to_datetime(df.DateTime).dt.strftime('%Y%m%d') + " " + df.tz + " DH"
                        + pd.to_datetime(df.DateTime).dt.strftime('%H%M')
                        + "/DUE /" + df.SHEF_PE + df.dur + type_source + "ZZ " + df.value.astype(str))
        elif dur_str == 'daily':
            out_lines = (".AR " + df.nws_id + " " 
                        + pd.to_datetime(df.DateTime).dt.strftime('%Y%m%d') + " " + df.tz + " DH24"
                        + "/DUE /" + df.SHEF_PE + df.dur + type_source + "ZZ " + df.value.astype(str))
            
        if shef_header == True:
            write_header(utc_now, new_fullfn, out_fmt)
            with open(new_fullfn, 'a') as f:
                f.write("\n".join(out_lines))
        elif shef_header == False: # do not append if no header
            with open(new_fullfn, 'w') as f:
                f.write("\n".join(out_lines))

    if os.path.isfile(last_fullfn):
        if out_fmt == "csv":
            write_header(utc_now, final_out_fullfn, out_fmt, header=csv_headers)
        if out_fmt == "shef" and shef_header == True:
            write_header(utc_now, final_out_fullfn, out_fmt)
        new_data = write_new_lines(last_fullfn, new_fullfn, final_out_fullfn, out_fmt)
    else:
        shutil.copyfile(new_fullfn, final_out_fullfn)
        new_data = True

    # removes duplicate lines within single file
    if os.path.isfile(final_out_fullfn):
        remove_dup_lines(final_out_fullfn)

    shutil.copyfile(new_fullfn, last_fullfn)
    if new_data:
        logging.info('usbr scraping complete with output to: ' + final_out_fullfn)
        logging.info('equivalent final output found in last file: ' + last_fullfn)

def main():
    utc_now = datetime.now(timezone.utc)
    arg_vals = parse_args()

    #map_df = pd.read_csv(os.path.join(meta_dir, post_control_fn))
    post_ctrl_df =  (pd.read_csv(os.path.join(meta_dir, post_control_fn), 
                                 skiprows=31,
                                 header=None, sep='\s+',
                                 names=['tz','partner_id','nws_id','exclude1','exclude2','exclude3']))
    map_df = post_ctrl_df.copy()[['partner_id', 'nws_id']]

    if arg_vals.duration == 'realtime':
        dur_id = 'I'
        alias_df = pd.read_csv(os.path.join(meta_dir, realtime_alias_fn))
        sites_df = pd.read_csv(os.path.join(meta_dir, realtime_site_obs_fn))
    elif arg_vals.duration == 'daily':
        dur_id = 'D'
        alias_df = pd.read_csv(os.path.join(meta_dir, daily_alias_fn))
        sites_df = pd.read_csv(os.path.join(meta_dir, daily_site_obs_fn))
    
    # ----- getting data, all calls by timezone
    # as of 2024-12-21, looks like most errors have been fixed:
    # previously if site/obs does not exist, multi call goes to UTC time
    logging.info('usbr scraping started')
    if arg_vals.locid == 'all':
        all_data_li = []
        all_unmapped = []
        for i, tz in enumerate(sites_df['tz'].unique()):
            # loop by timezone
            tz_data_li = []
            tz_sites_df = sites_df[sites_df['tz'] == tz]

            for j in range(0, len(tz_sites_df), max_call_num):
                # loop by max call
                subset_df = tz_sites_df.iloc[j:(j + max_call_num)]
                # site_sensor_str = subset_df['usbr_id'] + ' ' + subset_df['USBR_PE'] # old big call
                site_sensor_str = subset_df['usbr_id'].unique()
                call_data_df = get_data(site_sensor_str, dur_id, arg_vals.back)
                if call_data_df.empty == False:
                    tz_data_li.append(call_data_df)
            
            if len(tz_data_li) > 0:
                tz_data_df = reduce(lambda df1, df2: pd.merge(df1, df2, on='DateTime', how='outer'), tz_data_li)
                tz_unmapped, tz_org_df = org_data(tz_data_df, tz, alias_df, map_df, sites_df, dur_id)

                all_data_li.append(tz_org_df)
                all_unmapped.extend(tz_unmapped)
        all_data = pd.concat(all_data_li)
    else:
        usbr_id = map_df[map_df['nws_id'] == arg_vals.locid]['partner_id'] # returns object
        subset_df = sites_df[sites_df['usbr_id'].isin(usbr_id)]
        site_sensor_str = subset_df['usbr_id'] + ' ' + subset_df['USBR_PE']
        call_data_df = get_data(site_sensor_str, dur_id, arg_vals.back)
        all_unmapped, all_data = org_data(call_data_df, subset_df['tz'].unique()[0], alias_df, map_df, sites_df, dur_id)

    if len(all_unmapped) > 0:    
        logging.info('sites without mapped id, even BORXX: ' + ','.join(all_unmapped))

    # ----- output
    write_output(all_data, arg_vals.duration, utc_now)

if __name__ == '__main__':
    main()
