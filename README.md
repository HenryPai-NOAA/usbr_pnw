# usbr_pnw
Web scraper for usbr hydromet data

Description:
Makes multiple location calls, greatly speeding up downloads (20+ min vs. sub min)

Last update (2024-12-25):
- edited request header moved to config file
- parsing post_control.c5 file, but need to be careful if header lines changes number (skips 31 lines)

Need to define log and output directories and change some global vars accordingly.

Usage: 
- default args:   python get_usbr_shef.py --locid all --duration realtime --back 3
- daily call:     python get_usbr_shef.py --duration daily  <- minimalist, given default args are all and 3 
- station call:   python get_usbr_shef.py --locid LUCI1 --duration realtime --back 24
