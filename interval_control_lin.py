#!/usr/bin/python2.7

# DFN INTERVAL CONTROL - Controls DFN Observatory, takes pictures overnight.
# (C) Copyright 2017 Martin Towner/Ben Hartig/Desert Fireball Network

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


# This file is part of the Desert Fireball Network camera control system.

# Operation:
#     Uses tethered mode to capture images during evening twilight, night
#     and morning twilight. Each block can have different settings (e.g. jpg 
#     vs raw), but gphoto cannot be accessed during the tether. This version
#     is light in comparison to its predecessor. Background processing,
#     gphoto level and clearing quality have all been temporarily removed.

#     Run a default test with minimum night of 180s with:
#         python interval_control_lin.py test
#     Longer tests can be run with an additional argument (e.g. 240s)
#         python interval_control_lin.py test 240

# Requirements:
#     python 2.7+

# Dependencies:
#     gphoto2 (apt-get install gphoto2)
#     pyephem (pip install pyephem), which needs
#         python-pip (apt-get install python-pip), which needs
#             python-dev (apt-get install python-dev)
#     pyserial (pip install pyserial)

# Notes:
#     Uses tethered mode for three blocks: Evening Twilight, Night &
#         Morning Twilight.
#     Doesn't use the memory card in the camera. (Shouldn't need a card,
#         but have had some issues testing this previously.)
#     Only calls processing at the end of the night, still makes thumbs.
#     Directory (current_dir) is no longer changing for each mode.
#
#     gphoto2 --filename=foo fails if foo is on another filesystem;
#     http://sourceforge.net/p/gphoto/bugs/805/

# TODO: General Items
#     Replace removed gphoto level considering tethering, 
#         currently settings cannot be changed during a tether.
#         This could be remedied using gphoto shell mode to write 
#         custom tethers.
#     Replace removed clearing quality.
#     Check logging outputs and write wiki entry for general usage.
#     Test system without memory card in camera.
#     Check how many photos are on the camera after each block???

# TIP: Imports and declarations.   
# Imports from external.
from __future__ import print_function, division

import datetime
import logging
import os
import resource
import shutil
import subprocess
import sys
import time

# Imports from dfn.
import dfn_functions as dfn
import leostick as leo
import camera as cam
import sun_and_moon as sm
import cloudy_check as cc


# Version control to be included in logs.
VERSION = '2017-05-30_All_Night_Tether'

# Poolsize is number of processes.
POOLSIZE = 4

# Enumeration for cloud detection.
CLOUDY = 0
CLEARING = 1
CLEAR = 2

# Enumeration for gphoto settings.   
RAW_MODE = 4
JPG_MODE = 2

global ser

# TIP: Main function.
def main_function(test_time = 0):
    """Takes one night worth of images with processing at sunrise. 
    
       test_time -- the length for a test, 0 is no testing. (default 0)    
    """
    
    # Initialise variables.
    job_list = []
    
    # Get program directory.
    prog_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load new config file from master copy in /opt/dfn-software.
    config_file = os.path.join(prog_dir, r'dfnstation.cfg')
    config_dict = dfn.load_config(config_file)
    config_dict['internal']['config_file'] = config_file
               
    # Setup new data path for this night.
    data_path = dfn.make_data_path( config_dict['internal']['data_directory'])
    
    # TIP: Setup logger.
    # Set lowest log level for testing.
    if test_time != 0:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
        
    # Set path, format and identity for logging.    
    log_file = os.path.join( data_path, dfn.log_name()+'interval.txt')
    tether_file = os.path.join(data_path, dfn.log_name()+'tether.txt')
    formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(module)s, %(message)s')
    logger = logging.getLogger()
     
    # Remove any pre-existing handlers.
    if len(logger.handlers) != 0:
        for hdl in logger.handlers[:]:
            hdl.stream.close()
            logger.removeHandler(hdl)
            
    # Begin handler that records logs in /data0/latest.         
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.setLevel(log_level)

    # Provide logger details and stream logging to stdout for testing.
    if test_time != 0:
        print('logger_handler_count, ' + str(len(logger.handlers)))
        print('logfile, ' + log_file)
        logger.addHandler(logging.StreamHandler())
        logger.info('testing')        
       
    # Report interval control version.
    logger.info('interval_control_version, ' + VERSION)
    
    # Initialise microcontroller.
    ser = leo.connect_to_leostick()
    leo.shutter_off()
    leo.wait_for_camera_ready()
    leo.camera_off()
    leo.video_off()
    
    # Set bulb mode in microcontroller based on config.
    if config_dict['camera']['exp_mode'] == 'BULB':
        leo.set_bulb_mode()
        logger.info('bulb_mode')
    else:
        leo.set_non_bulb_mode()
        logger.info('non_bulb_mode')
    
    # Find latest image mask for the dslr and make a local copy.
    maskfile = dfn.get_mask( config_dict['internal']['data_directory'] )
    shutil.copy( maskfile, data_path )
    logger.debug('mask_copied, ' + data_path)
    logger.info('mask, ' + maskfile)
    
    # FIXME: Is this still needed if processing is a daemon?
    # Make the transfer file early to allow background event detection.
    transfer_status_file = os.path.join( data_path, r'transfer_status.txt')
    dfn.write_string_to_file( 'unprocessed\n', transfer_status_file, mode = 'wt')
    logger.debug( 'transfer_status_file_written, unprocessed')
        
    # Get new gps location, if available and report lock.
    (config_dict['station']['lon'],
     config_dict['station']['lat'],
     config_dict['station']['altitude'],
     config_dict['station']['gps_lock']) = leo.update_GPS_location(
                                           config_dict['station']['lon'],
                                           config_dict['station']['lat'],
                                           config_dict['station']['altitude'])
    logger.info('GPS_lonlat, ' +
                str(config_dict['station']['lon']) + ', ' +
                str(config_dict['station']['lat']) + ', ' +
                str(config_dict['station']['altitude']) + ', ' +
                str(config_dict['station']['gps_lock']))

    # TIP: Calculate sunset and sunrise in localtime.
    sunrise, sunset, moonrise, moonset = sm.generate_sun_and_moon(
                                         config_dict['station']['lon'],
                                         config_dict['station']['lat'])
    sunset += datetime.timedelta(minutes = float(
                                           config_dict['internal']['sun_leeway']))
    sunrise -= datetime.timedelta(minutes = float(
                                            config_dict['internal']['sun_leeway']))
    sunset_after_twilight = sunset + datetime.timedelta(minutes=10)
    sunrise_before_twilight = sunrise - datetime.timedelta(minutes=10)
    
    # Set dummy values for testing.
    if test_time != 0:
        sunset = datetime.datetime.now() + datetime.timedelta(seconds=30)
        sunrise = sunset + datetime.timedelta(seconds=test_time)
        sunset_after_twilight = sunset
        sunrise_before_twilight = sunrise
    logger.info('sunset, ' + str(sunset.isoformat()))
    logger.info('sunset_after_twilight, ' + str(sunset_after_twilight.isoformat()))
    logger.info('sunrise_before_twilight, ' + str(sunrise_before_twilight.isoformat()))
    logger.info('sunrise, ' + str( sunrise.isoformat()))
    logger.info('now, ' + str(datetime.datetime.now()))
    logger.info('UTCnow, ' + str(datetime.datetime.utcnow()))
    logger.info('timezone, ' + str(time.timezone))
    
    # Handle daylight savings.
    if time.daylight != 0:
        logger.info('altzone, ' + str(time.altzone))
        
    # Handle missed sunset, force an immediate start.
    if sunset >= sunrise:
        logger.info('late_start-forcing_immediate')
        sunset = datetime.datetime.now() + datetime.timedelta(seconds=30)

    # Use cal.mktime not timegm as sunrise is local datetime object.
    sunrise_epoch = time.mktime(datetime.date.timetuple(sunrise))
    sunset_epoch = time.mktime(datetime.date.timetuple(sunset))
    
    # Convert back again for testing.
    sunrise_recalc_test = time.localtime(sunrise_epoch)
    logger.debug('recalc_sunrise, ' + str(sunrise_epoch) + ', '
                                    + str(sunrise_recalc_test))

    # TIP: Wait until sunset.
    while datetime.datetime.now() < sunset:
        logger.debug('waiting_for_sunset, ' + datetime.datetime.now().isoformat())
        print('waiting_for_sunset, ' + datetime.datetime.now().isoformat())
        time.sleep(30)
        
        # If no lock try again.
        if config_dict['station']['gps_lock'] == 'N' and test_time == 0:
            (config_dict['station']['lon'],
            config_dict['station']['lat'],
            config_dict['station']['altitude'],
            config_dict['station']['gps_lock']) = leo.update_GPS_location(
                                                  config_dict['station']['lon'],
                                                  config_dict['station']['lat'],
                                                  config_dict['station']['altitude'])
            
            # If new lock handle coordinates and recalculate timing.
            if config_dict['station']['gps_lock'] != 'N':
                logger.info('GPS_lonlat, ' +
                            str(config_dict['station']['lon']) + ', ' +
                            str(config_dict['station']['lat']) + ', ' +
                            str(config_dict['station']['altitude']) + ', ' +
                            str(config_dict['station']['gps_lock']) )
                
                # Recalculate sunset and sunrise in localtime.
                sunrise, sunset, moonrise, moonset = sm.generate_sun_and_moon(
                                                     config_dict['station']['lon'],
                                                     config_dict['station']['lat'])
                sunset += datetime.timedelta(minutes = float(
                                                       config_dict['internal']['sun_leeway']))
                sunrise -= datetime.timedelta(minutes = float(
                                                        config_dict['internal']['sun_leeway']))
                sunset_after_twilight = sunset + datetime.timedelta(
                                                minutes=10)
                sunrise_before_twilight = sunrise - datetime.timedelta(
                                                minutes=10)
                
                # Set dummy values for testing.
                if test_time != 0:
                    sunset = datetime.datetime.now() + datetime.timedelta(seconds=30)
                    sunrise = sunset + datetime.timedelta(seconds=test_time)
                    sunset_after_twilight = sunset
                    sunrise_before_twilight = sunrise
                logger.info('sunset, ' + str(sunset.isoformat()))
                logger.info('sunset_after_twilight, ' + str(sunset_after_twilight.isoformat()))
                logger.info('sunrise_before_twilight, ' + str(sunrise_before_twilight.isoformat()))
                logger.info('sunrise, ' + str( sunrise.isoformat()))
                logger.info('now, ' + str(datetime.datetime.now()))
                logger.info('UTCnow, ' + str(datetime.datetime.utcnow()))
                logger.info('timezone, ' + str(time.timezone))
                
                # Handle daylight savings.
                if time.daylight != 0:
                    logger.info('altzone, ' + str(time.altzone))
        
                # Handle missed sunset, force an immediate start.
                if sunset >= sunrise:
                    logger.info('late_start-forcing_immediate')
                    sunset = datetime.datetime.now() + datetime.timedelta(seconds=30)

                # Use cal.mktime not timegm as sunrise is local datetime object.
                sunrise_epoch = time.mktime(datetime.date.timetuple(sunrise))
                sunset_epoch = time.mktime(datetime.date.timetuple(sunset))
    
                # Convert back again for testing
                sunrise_recalc_test = time.localtime(sunrise_epoch)
                logger.debug('recalc_sunrise, ' + str(sunrise_epoch) + ', '
                                                + str(sunrise_recalc_test))                
    # Report sunset time.
    logger.debug('sunset_now, ' + str(datetime.datetime.now()))
    
    # Get initial status, versions, temperature, etc.
    temperature = leo.get_temperature()
    logger.info('leostick_temperature, ' + str(temperature))
    time.sleep(1)
    leo_version = leo.get_version()
    logger.info('leostick_version, ' + str(leo_version))
    time.sleep(1)
    leo_sequence = leo.get_sequence()
    logger.info('leostick_sequence, ' + str(leo_sequence))
    time.sleep(1)
    leo_debug = leo.get_debug_codes()
    logger.info('leostick_debug, ' + str(leo_debug))
    time.sleep(1)
    logger.info('cloud_file, ' + config_dict['internal']['cloudy_img_file'])
    logger.info('HD_temperature, ' + str( dfn.disk_temperature()))
    logger.info('today_date, ' + dfn.today())
    logger.info('data_path, ' + data_path)
    logger.info('test_time, ' + str(test_time))
    mem_use = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    logger.info('memory, ' + "{:,}".format(mem_use))
    for item in dfn.get_ntp_data():
            logger.info('ntp, ' + str(item))

    # Switch on hardware.
    # FIXME: Handle point grey video camera.    
    time.sleep(10)
    if os.path.exists(r'/dev/video0'):
        config_dict['camera']['video_device_exists'] = 1
        logger.info('video_device_found')
        if str(config_dict['camera']['video_enabled']) != '0':
            if test_time == 0: #start up video cloud daemon
                logger.info('spawning_video')
                cc.spawn_video_command(sunrise_epoch)
            leo.video_on()
            logger.info('video_on_ok')
        else:
            logger.info('video_not_enabled')
    else:
        config_dict['camera']['video_device_exists'] = 0
        logger.info('video_not_exist')
    
    # Switch on camera and initialise settings. 
    leo.camera_on()
    logger.info('camera_on_ok')
    os.chdir(data_path)
    cam.camera_download_images()
    dfn.rename_RAW_all(data_path, config_dict)
    cam.camera_set_time()
    time.sleep(1)
    logger.info('camera_time_get, ' + cam.camera_get_time())
    cam.camera_set_program() # manual exp mode
    cam.camera_set_autoiso(1) # autoiso off
    cam.camera_set_highisonr()
    cam.camera_set_longexpnr()
    cam.camera_set_vignette()
    cam.camera_set_fstop( config_dict['camera']['camera_fstop'])
    
    # Switch on condensation fan.
    leo.cond_on()
    
    # FIXME: This can be removed for the DFNEXT.
    if 'firmware_control' in config_dict:
        if (config_dict['firmware_control']['heater_enabled'] == '1' or
            config_dict['firmware_control']['heater_enabled'] == 1 or
            config_dict['firmware_control']['heater_enabled'] == True ):
            leo.heater_on( config_dict['firmware_control']['heater_temperature_C'])
    config_dict['camera']['shutterspeed'] = cam.get_camera_shutterspeed()

    # Save a local copy of the config file.
    if dfn.save_config_file( os.path.join(data_path,'dfnstation.cfg'), config_dict):
        logger.info('new_conf_file_written')
    else:
        logger.warning('new_conf_file_write_error')
    logger.info('location, ' + config_dict['station']['location'])
    
    # Set interval time from config unless testing.
    interval_time = str(int(config_dict['clouds']['time_checking_clear']))
    interval_time.rstrip('s') #XXX: Is this required? Already typecast to int...
    
    # Set fixed interval length, tests will run for 
    # config time + n * interval_time until passed test_time.
    if test_time != 0:        
        interval_time = '180' 
        
    # Set directory.
    current_dir = data_path
    os.chdir(current_dir)
    
    # Start with incorrect value to force immediate mode change
    cloud_status_internal = -3
    
    # TIP: Evening Twilight _______________________________________________________________________
    if test_time == 0:
        logger.info('twilight_evening_settings')   
        
        cam.camera_set_quality(JPG_MODE)
        
        # TODO: Remove config check and ensure defaults in config.
        if 'twilight_exposuretime' in config_dict['camera']:
            cam.camera_set_shutter(config_dict['camera']['twilight_exposuretime'])
        else:
            cam.camera_set_shutter(config_dict['camera']['camera_exposuretime'])
        if 'twilight_iso' in config_dict['camera']:
            cam.camera_set_iso(config_dict['camera']['twilight_iso'])
        else:
            cam.camera_set_iso(config_dict['camera']['camera_iso'])
    
        
        logger.info('twilight_evening_starting')

        # FIXME: Evening Tether! This needs to be tested, needs to call in the background.
        with open(os.devnull, 'w') as shutup:
            try:
                # Calculate number of seconds in twilight for tethering.
                evening_twilight_seconds = int((sunset_after_twilight 
                                                - datetime.datetime.now()).total_seconds())
                subprocess.Popen(['gphoto2', '--capture-tethered',
                                 str(evening_twilight_seconds) + 's', '--force-overwrite'],
                                 stderr = shutup, close_fds = True)
                logger.info('evening_twilight_tether_starting ' + str(evening_twilight_seconds) + 's')
            except subprocess.CalledProcessError as e:
                logger.warning('argh-problem_starting_tether, ' + str(e))
                print('evening_twilight_tether_err, ' + str(datetime.datetime.now()) + ', ' + str(e))
                res = subprocess.call(['gphoto2', '--reset'],
                                      stderr = shutup)
                logger.info('gphoto_reset, ' + str(res))

        while datetime.datetime.now() < sunset_after_twilight:
            imgfile = high_acq(current_dir, interval_time, config_dict)
            handle_new_image(imgfile, job_list, current_dir, config_dict)
            
    else:
        logger.debug('evening_twilight_not_called')

    # TIP: Night __________________________________________________________________________________
    logger.info('night_settings')

    cam.camera_set_quality(RAW_MODE)
    
    cam.camera_set_shutter(config_dict['camera']['camera_exposuretime'])
    cam.camera_set_iso(config_dict['camera']['camera_iso'])

    logger.info('night_starting')

    # FIXME: Night Tether! This needs to be tested, needs to call in the background.
        
    with open(tether_file, 'w') as shutup:
        try:
            # Calculate number of seconds in night for tethering.
            night_seconds = int((sunrise_before_twilight 
                                 - datetime.datetime.now()).total_seconds())
            tether = subprocess.Popen(['gphoto2', '--capture-tethered', '--force-overwrite'],
                             stderr = shutup, close_fds = True)
            logger.info('night_tether_starting, ' + str(night_seconds) + 's')
        except subprocess.CalledProcessError as e:
            logger.warning('argh-problem_starting_tether, ' + str(e))
            print('night_tether_err, ' + str(datetime.datetime.now()) + ', ' + str(e))
            res = subprocess.call(['gphoto2', '--reset'],
                                  stderr = shutup)
            logger.info('gphoto_reset, ' + str(res))

    while datetime.datetime.now() < sunrise_before_twilight:      
        # Collect and report current cloud status.
        cloud_status = cc.read_cloud_status(config_dict['internal']['cloud_status_file'])
        print(datetime.datetime.now(), 'sunrise, ' + str(sunrise)
                + ', ' + str(cloud_status))
        logger.debug('cloud_status, ' + str(cloud_status) + ', ' + str(cloud_status_internal))
        
        # Force status CLEAR for testing.        
        if test_time != 0:
            logger.debug('testing_force_clear')
            cloud_status = CLEAR
        
        # Select acquisition based on cloud status.         
        if cloud_status == CLEAR:
            if cloud_status_internal != CLEAR:
                logger.info('Gone_clear, ' + str(cloud_status))
            imgfile = high_acq(current_dir, interval_time, config_dict) 
        elif cloud_status == CLEARING:
            if cloud_status_internal != CLEARING:
                logger.info('Gone_clearing, ' + str(cloud_status))
            imgfile = low_acq(current_dir, cloud_status, config_dict)
        elif cloud_status == CLOUDY:
            logger.info('Gone_cloudy, ' + str(cloud_status))
            imgfile = low_acq(current_dir, cloud_status, config_dict)
        else: 
            if cloud_status_internal in (CLEAR, CLEARING, CLOUDY):
                logger.info('Gone_undefined_cloud_status, ' + str(cloud_status))
            else:
                logger.info('Still_undefined_cloud_status, ' + str(cloud_status))
                # TODO: Incorporate uncertainty measures for undefined status.
            imgfile = high_acq(current_dir, interval_time, config_dict)
        cloud_status_internal = cloud_status
    
    time.sleep(5) 
    
    print(str(tether.poll()))    
    if tether.poll() is None:
        logger.debug('Tether_not_killed, ' + str(tether.pid))
    else:
        logger.debug('Tether_killed, ' + str(tether.pid))
    
    tether.terminate()
    tether.kill()
    
    
    print(str(tether.poll()))      
    if tether.poll() is None:
        logger.debug('Tether_not_killed, ' + str(tether.pid))
    else:
        logger.debug('Tether_killed, ' + str(tether.pid))

    # TIP: Morning Twilight _______________________________________________________________________
    if test_time == 0:
        logger.info('twilight_morning_settings')
        cam.camera_set_quality(JPG_MODE)
        # TODO: Remove config check and ensure defaults in config.
        if 'twilight_exposuretime' in config_dict['camera']:
            cam.camera_set_shutter(config_dict['camera']['twilight_exposuretime'])
        else:
            cam.camera_set_shutter(config_dict['camera']['camera_exposuretime'])
        if 'twilight_iso' in config_dict['camera']:
            cam.camera_set_iso(config_dict['camera']['twilight_iso'])
        else:
            cam.camera_set_iso(config_dict['camera']['camera_iso'])
        
        logger.info('twilight_morning_start')

        # Morning Tether! Start gphoto tether in background until sunrise. 
        with open(os.devnull, 'w') as shutup:
            try:
                # Calculate number of seconds in night for tethering.
                morning_twilight_seconds = int((sunrise 
                                                - datetime.datetime.now()).total_seconds())
                
                subprocess.Popen(['gphoto2', '--capture-tethered',
                                 str(morning_twilight_seconds) + 's', '--force-overwrite'],
                                 stderr = shutup, close_fds = True)
                logger.info('morning_twilight_tether_starting ' + str(morning_twilight_seconds) + 's')
            except subprocess.CalledProcessError as e:
                logger.warning('argh-problem_starting_tether, ' + str(e))
                print('morning_twilight_tether_err, ' + str(datetime.datetime.now()) + ', ' + str(e))
                res = subprocess.call(['gphoto2', '--reset'],
                                      stderr = shutup)
                logger.info('gphoto_reset, ' + str(res))

        while datetime.datetime.now() < sunrise:
            imgfile = high_acq(current_dir, interval_time, config_dict)
            handle_new_image(imgfile, job_list, current_dir, config_dict)
    else:
        logger.debug('morning_twilight_not_called')

    # TIP: Sunrise
    logger.info('sunrise')

    # Shutdown hardware.
    leo.shutter_off()
    if str(config_dict['camera']['video_device_exists']) == '1':
        leo.video_off()
        logger.info('video_off_ok')
        
    # Wait for last shutter exposure to finish.
    leo.wait_for_camera_ready()
    
    # Wait for camera to record last image from buffer.
    time.sleep(10)
    for job in job_list:
        job.join()
    logger.info('finished_outstanding_tasks')
    os.chdir(data_path)
    
    # Clear any stray images from memory card.
    cam.camera_download_images()

    leo.camera_off()
    logger.info('camera_off_ok')
    leo.cond_off()

    ser.close()
    
    # Clean up images.
    renamed_images = dfn.rename_RAW_all(data_path, config_dict)
    logger.info('rename_RAW_all_ok, ' + str(len(renamed_images)))
    if str(config_dict['internal']['clearing_quality']) == '2':
        dfn.make_all_thumb( data_path)
        logger.info('make_all_thumb_ok')
    else:
        logger.debug('make_all_thumb_not_called')
        
    # Get a rough shutter count just listdir then sort by cdate.
    raw_list = [os.path.join(data_path,a) for a in os.listdir(data_path)
                                                if (a.lower().endswith('.nef') or
                                                    a.lower().endswith('.cr2') ) ]
                                                
    # Check there are actually images.
    if len(raw_list) > 1: 
        last_image = sorted( raw_list,
                key=lambda x: os.stat(os.path.join(data_path,x)).st_mtime)[-1]
        logger.info( 'shuttercount, '+str( dfn.image_shuttercount(last_image)))
        
    # Report memory usage.
    mem_use = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    logger.info( 'memory, ' + "{:,}".format(mem_use) )
    logger.info( 'today_disk_usage, ' + "{:,}".format(dfn.disk_usage( data_path)) )
    for item in dfn.get_ntp_data():
        logger.info( 'ntp, ' + str(item) )
    time.sleep( 60)

    # Close down logging and exit.
    if test_time == 0:
        if ( (not 'enabled' in config_dict['event_detect']) or
            (config_dict['event_detect']['enabled'] != '0' and
             config_dict['event_detect']['enabled'] != 'N' ) ):
            logger.info( 'exiting_interval_control_calling_processing' )
            
            # Camera systems do a reboot at 1615 localtime.
            reboot_time = dfn.get_reboot_time()
            reboot_time_epoch = time.mktime( datetime.date.timetuple( reboot_time) )
            os.execvp( sys.executable,
                       [ sys.executable, r'/opt/dfn-software/processing_wrapper.py',
                         '/data0/latest', str(reboot_time_epoch), '012346'])
        else:
            logger.info( 'exiting_interval_control_processing_not_enabled' )        
    else:
        logger.info( 'finished_interval_control_night_end_test' )
        
    sys.stdout.flush()
    sys.stderr.flush()
    logging.shutdown()
    return True


def high_acq(current_dir, interval_time, config_dict):
    """carry out high acq for one unit of time
       start normal high rate for x sec, all night tethered mode
       """
    logger = logging.getLogger()
    logger.info('starting_high_acq_chunk, ' + interval_time + ', ' + current_dir)
    
    leo.shutter_on()
    print('hr,', datetime.datetime.now() )
    time.sleep(int(interval_time))
    leo.shutter_off()
        
    #FIXME: Count is zero, checked path, syntax and timing...
    imgcnt = len([a for a in os.listdir(current_dir) if ('nef' in a.lower())])
    logger.info('finished_high_acq_chunk, img_count, ' + str(imgcnt))
    
    renamed_images = dfn.rename_RAW_all(current_dir, config_dict)
    logger.debug('renamed_images, ' + str(renamed_images))
    
    thumbed_images = dfn.make_all_thumb(current_dir, images_to_do = renamed_images)
    logger.debug('thumbed_images, ' + str(thumbed_images))
    
    imgfile = dfn.get_latest_imagefile(current_dir)
    logger.info('finished_rename_get_latest, ' + imgfile)
    
    return imgfile


def low_acq(current_dir, cloud_status, config_dict):
    """after specified period take single image and output for cloudiness"""
    logger = logging.getLogger()
    
    if cloud_status == CLOUDY:
        sleep_time = config_dict['clouds']['time_checking_cloudy']
    elif cloud_status == CLEARING:
        sleep_time = config_dict['clouds']['time_checking_clearing']
    else:
        sleep_time = 30
        
    logger.info('starting_low_acq, ' + sleep_time + current_dir)
    time.sleep(int(sleep_time))
    leo.single_image()
    
    #TODO: Replace temporary image handling stolen from high_acq with appropriate low_acq handling.
    renamed_images = dfn.rename_RAW_all(current_dir, config_dict)
    thumbed_images = dfn.make_all_thumb(current_dir, images_to_do = renamed_images)
    imgfile = dfn.get_latest_imagefile(current_dir)
    logger.debug('finished_low_acq, ' + imgfile)
    
    return imgfile
 
    
# FIXME: Check for compatibility and usage.
def handle_new_image( imgfile, job_list, current_dir, config_dict):
    """handle a new image"""
    logger = logging.getLogger()
    if os.path.isfile( imgfile):
        logger.info('handle-image_starting, ' + imgfile)
        if dfn.detect_disk_full(): #hd nearly full!
            logger.critical('disk-full_early-exit')
            return clean_up_no_dl(job_list)
        #img for specific cloudy detection has cloudy in name
        if 'cloudy_img' in imgfile or 'clearing' in imgfile:
            logger.info('handle-image_cloudy_and_clearing, ' + str(imgfile) )
            result = dfn.write_last_image_file( imgfile,
                                config_dict['internal']['last_img_status_file'])
        else: # its a RAW or JPG proper taken image
            imgfile = dfn.rename_RAW( imgfile, config_dict)
            if imgfile.lower().endswith('.jpg'):
                logger.info('handle-image_jpg, ' + str(imgfile) )
                if not 'thumb.jpg' in imgfile:
                    #its a .JPG - either twilight or jpg clearing mode
                    if config_dict['internal']['clearing_quality'] == '2':
                        #its a twilight JPG, need to make thumb.jpg for detection
                        imgfile = dfn.make_thumb( imgfile)
                        logger.info('handle-image_thumb-jpg, ' + str(imgfile) )
                result = dfn.write_last_image_file( imgfile,
                                config_dict['internal']['last_img_status_file'])
                ############# just for testing of file format
                dummy = dfn.write_last_image_file( imgfile,
                                r'/tmp/latest_img.tmp')
                #############
            else: #clearing raw mode or high rate raw
                logger.info( 'making_thumb_for_cloudy, ' + str(imgfile) )
                imgfile = dfn.make_thumb( imgfile)
        imgsize = os.stat( imgfile).st_size
        logger.info( 'latest_image, %s, %s, %s' %
                 (str(imgfile), dfn.exposure_time( imgfile, True),
                  str(imgsize)) )
    else:
        logger.info( 'argh-imgfile_not_exist, ' + current_dir + ', '
                    + str(imgfile) )
    return

# FIXME: Check for compatibility and usage.
def move_jpg_to_cloudy( current_dir, imgfile, config_dict):
    """rename an imagefile to the cloud file name given by config_dict"""
    logger = logging.getLogger()
    imgfile = os.path.join( current_dir, imgfile)
    if os.path.isfile( imgfile):
        shutil.move( imgfile, config_dict['internal']['cloudy_img_file'])
        logger.info('moved imgfile, %s, %s' %
            (imgfile, config_dict['internal']['cloudy_img_file']) )
        imgfile = config_dict['internal']['cloudy_img_file']
        logger.info('slow_dl_image2, ' + imgfile )
    else:
        logger.info('slow_dl_not-valid-file, ' + imgfile)
    return imgfile


# FIXME: Check for compatibility and usage.
def clean_up_no_dl( job_list = []):
    leo.shutter_off()
    leo.cond_off()
    leo.wait_for_camera_ready()
    leo.video_off()
    time.sleep(10) #wait for camera to record last image from buffer to CF card
    leo.camera_off()
    for job in job_list: #finish off any running mask conversions
        job.join()
    ser.close()
    sys.stdout.flush()
    sys.stderr.flush()
    logging.shutdown()
    return False


# FIXME: Give credit? 
def check_pid(pid):        
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == 'test':
            print( sys.argv[1])
            if len(sys.argv) > 2:
                test_time = int( sys.argv[2])
            else:
                test_time = 180
            if test_time < 180:
                test_time = 180
    else:
        test_time = 0
    main_function( test_time)
    sys.exit(0)
