# usbr_pnw
Web scraper for usbr hydromet data

Description:
Makes multiple location calls, greatly speeding up downloads (20+ min vs. sub min).  Can also perform single station calls (slower).  Reminder, units are defined in the shef manual [1], table 1, pgs 4-5 (pgs 75-76 in pdf) 

Last update (2024-12-26):
- edited request header moved to config file
- parsing post_control.c5 file, but need to be careful if header lines changes number (skips 31 lines)

Steps:
- Need to define log and output directories and change some global vars accordingly.
- Make config.yaml file with request header info by adding line (copy & paste bold, edit bracket info to be more descriptive): **user_agent : '[user info to pass]'**


Usage: 
- default args:   python get_usbr_shef.py --locid all --duration realtime --back 3
- daily call:     python get_usbr_shef.py --duration daily  <- minimalist, given default args are 'all' and '3' (days for daily call) 
- station call:   python get_usbr_shef.py --locid LUCI1 --duration realtime --back 24

References:
- shef manual: https://www.weather.gov/media/mdl/SHEF_CodeManual_5July2012.pdf

Todo:
- [x] more robust parsing for post_control.c5, but would like to check with how this list is generated with USBR, USACE, and BCHydro
