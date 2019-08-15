"""
Description:
    Download and store raw JSON channel history for joined standard channels and direct messages.
    Specify start and/or end date bounds or use defaults of room creation and yesterday (respectively)

Dependencies:
    pipenv install
        rocketchat_API - Python API wrapper for Rocket.Chat
            https://github.com/jadolg/rocketchat_API
            ( pipenv install rocketchat_API )
        
    Actual Rocket.Chat API
        https://rocket.chat/docs/developer-guides/rest-api/channels/history/

Configuration:
    settings.cfg contains Rocket.Chat login information and file paths

Commands:
    pipenv run python export-history.py settings.cfg
    pipenv run python export-history.py -s 2000-01-01 -e 2018-01-01 -r settings.cfg
    etc
    
Notes:
    None

Author:
    Ben Willard <willardb@gmail.com> (https://github.com/willardb)
"""

from rocketchat_API.rocketchat import RocketChat
from time import sleep
import datetime
import pickle
import os
import logging
import pprint
import argparse
import configparser

#
# Initialize stuff
#
VERSION = 1.1

date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
short_date_format = "%Y-%m-%d"
one_day = datetime.timedelta(days=1)
today = datetime.datetime.today()
yesterday = today - one_day
null_date = datetime.datetime(1, 1, 1, 0, 0, 0, 0)
room_state = {}


# args
argparser_main = argparse.ArgumentParser()
argparser_main.add_argument('configfile', help='Location of configuration file')
argparser_main.add_argument('-s', '--datestart', help='Datetime to use for global starting point e.g. 2016-01-01 (implied T00:00:00.000Z)')
argparser_main.add_argument('-e', '--dateend', help='Datetime to use for global ending point e.g. 2016-01-01 (implied T23:59:59.999Z)')
argparser_main.add_argument('-r', '--readonlystate', help='Do not create or update history state file.', action="store_true")
args = argparser_main.parse_args()

start_time = datetime.datetime.strptime(args.datestart,short_date_format).replace(hour=0, minute=0, second=0, microsecond=0) if args.datestart else None
end_time = datetime.datetime.strptime(args.dateend,short_date_format).replace(hour=23, minute=59, second=59, microsecond=999999) if args.dateend else yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)


# config
config_main = configparser.ConfigParser()
config_main.read(args.configfile)

polite_pause = int(config_main['rc-api']['pause_seconds'])
count_max = int(config_main['rc-api']['max_msg_count_per_day'])
output_dir = config_main['files']['history_output_dir']
state_file = config_main['files']['history_statefile']

rc_user = config_main['rc-api']['user']
rc_pass = config_main['rc-api']['pass']
rc_server = config_main['rc-api']['server']


# logging
logger = logging.getLogger('export-history')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('export-history.log')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


#
# Functions
#
def get_rocketchat_timestamp(in_date):
    s = in_date.strftime(date_format)
    return s[:-4] + 'Z'
    
def assemble_state(state_array,room_json,room_type):
    for channel in room_json[room_type]:
        if channel['_id'] not in state_array:
            state_array[channel['_id']] = {
                'name': channel['name'] if 'name' in channel else 'direct-'+channel['_id'],
                'type': room_type,
                'lastsaved': null_date,
                'begintime':datetime.datetime.strptime(channel['ts'], date_format).replace(hour=0, minute=0, second=0, microsecond=0)
            }
        # Channel with no messages don't have a lm field
        if channel.get('lm'):
            lm = datetime.datetime.strptime(channel['lm'], date_format)
        else:
            lm = null_date
        state_array[channel['_id']]['lastmessage'] = lm
        
def upgrade_state_schema(state_array,old_schema_version):
    cur_schema_version = old_schema_version
    logger.info('State schema version of ' + str(schema_version) + ' is less than current version of ' + str(VERSION))
    if schema_version < 1.1:
        logger.info('Upgrading ' + str(cur_schema_version) + ' to 1.1...')
        # 1.0->1.1 update values for 'type' key
        t_typemap = {'direct':'ims','channel':'channels'}
        for t_id in state_array:
            state_array[t_id]['type'] = t_typemap[state_array[t_id]['type']]
        state_array['_meta'] = {'schema_version':1.1}
        logger.info('Finished ' + str(cur_schema_version) + ' to 1.1...')
        cur_schema_version = state_array['_meta']['schema_version']
        logger.debug('\n' + pprint.pformat(state_array))
        
    
# 
# Main
#
logger.info('BEGIN execution at ' + str(datetime.datetime.today()))
logger.debug('Command line arguments: ' + pprint.pformat(args))

if args.readonlystate:
    logger.info('Running in readonly state mode. No state file updates.')

if os.path.isfile(state_file):
    logger.debug('LOAD state from ' + state_file)
    sf = open(state_file,'rb')
    room_state = pickle.load(sf)
    sf.close()
    logger.debug('\n' + pprint.pformat(room_state))
    schema_version = 1.0 if '_meta' not in room_state else room_state['_meta']['schema_version']
    if schema_version < VERSION:
        upgrade_state_schema(room_state,schema_version)
    
else:
    logger.debug('No state file at ' + state_file + ', so state will be created')
    room_state = {'_meta': {'schema_version':VERSION}}

logger.debug('Initialize rocket.chat API connection')
rocket = RocketChat(rc_user, rc_pass, server_url=rc_server)

logger.debug('LOAD / UPDATE room state')
assemble_state(room_state,rocket.channels_list_joined().json(),'channels')
assemble_state(room_state,rocket.im_list().json(),'ims')
assemble_state(room_state,rocket.groups_list().json(),'groups')



for channel_id,channel_data in room_state.items():
    if channel_id != '_meta': # skip state metadata which is not a channel
        logger.info('------------------------')
        logger.info('Processing room: ' + channel_id + ' - ' + channel_data['name'])
        
        logger.debug('Global start time: ' + str(start_time))
        logger.debug('Global end time: ' + str(end_time))
        logger.debug('Room start ts: ' + str(channel_data['begintime']))
        logger.debug('Last message: ' + str(channel_data['lastmessage']))
        logger.debug('Last saved: ' + str(channel_data['lastsaved']))

        if start_time is not None:
            # use globally specified start time but if the start time is before the channel existed, fast-forward to its creation
            t_oldest = channel_data['begintime'] if channel_data['begintime'] > start_time else start_time
        elif channel_data['lastsaved'] != null_date:
            # no global override for start time, so use a tick after the last saved date if it exists
            t_oldest = channel_data['lastsaved']+datetime.timedelta(microseconds=1)
        else:
            # nothing specified at all so use the beginning time of the channel
            t_oldest = channel_data['begintime']
            
        t_latest = null_date
        
        if (t_oldest < end_time) and (t_oldest < channel_data['lastmessage']):
            logger.info('Grabbing messages since ' + str(t_oldest) + ' through ' + str(end_time))
        else:
            logger.info('Nothing to grab between ' + str(t_oldest) + ' through ' + str(end_time))
        
        while (t_oldest < end_time) and (t_oldest < channel_data['lastmessage']):
            logger.info('')
            t_latest = t_oldest + one_day - datetime.timedelta(microseconds=1)
            logger.info('start: ' + get_rocketchat_timestamp(t_oldest))
            
            history_data_obj = {}
            if (channel_data['type'] == 'channels'):
                history_data_obj = rocket.channels_history(channel_id, count=count_max, include='true',latest=get_rocketchat_timestamp(t_latest),oldest=get_rocketchat_timestamp(t_oldest))
            elif (channel_data['type'] == 'ims'):
                history_data_obj = rocket.im_history(channel_id, count=count_max, include='true',latest=get_rocketchat_timestamp(t_latest),oldest=get_rocketchat_timestamp(t_oldest))
            elif (channel_data['type'] == 'groups'):
                history_data_obj = rocket.groups_history(channel_id, count=count_max, include='true',latest=get_rocketchat_timestamp(t_latest),oldest=get_rocketchat_timestamp(t_oldest))
                
            history_data = history_data_obj.json()
            history_data_text = history_data_obj.text
            
            num_messages = len(history_data['messages'])
            logger.info('Messages found: ' + str(num_messages))
            
            if num_messages > 0:
                with open(output_dir + t_oldest.strftime('%Y-%m-%d') + '-' + channel_data['name'] + '.json','w') as f:
                     f.write(history_data_text.encode('utf-8').strip())
                sleep(polite_pause)
            elif num_messages > count_max:
                logger.error('Too many messages for this room today. SKIPPING.')
                
            logger.info('end: ' + get_rocketchat_timestamp(t_latest))
            logger.info('')
            t_oldest += one_day
            sleep(polite_pause)
                
        room_state[channel_id]['lastsaved'] = t_latest if t_latest > room_state[channel_id]['lastsaved'] else room_state[channel_id]['lastsaved']
        logger.info('------------------------\n')

if not args.readonlystate:
    logger.debug('UPDATE state file')
    logger.debug('\n' + pprint.pformat(room_state))
    sf = open(state_file,'wb')
    pickle.dump(room_state,sf)
    sf.close()
else:
    logger.debug('Running in readonly state mode: SKIP updating state file')

logger.info('END execution at ' + str(datetime.datetime.today()) + '\n------------------------\n\n')
