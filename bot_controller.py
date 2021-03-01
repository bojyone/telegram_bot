from telegram.ext import Updater, CommandHandler, CallbackContext, PollAnswerHandler, PollHandler
from telegram import Update, InputMediaPhoto
import logging
import mysql.connector
import json
import datetime
from datetime import datetime
import time
import os


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

logger = logging.getLogger(__name__)

updater = Updater('token', use_context=True)
j = updater.job_queue

cnx = mysql.connector.connect(user='***', password='***',
                              host='127.0.0.1',
                              database='telegram_bot',
                              use_unicode=True,
                              charset='utf8')

cursor = cnx.cursor(buffered=True)

photo_num = 0

message_id_1 = 0
message_id_2 = 0
message_id_3 = 0

msg_ids = [message_id_1, message_id_2, message_id_3]
poll_start = 0


def regular_poll(context: CallbackContext):

    f_json = open('file.json', 'r')
    data = f_json.read()
    data_json = json.loads(data)
    f_json.close()
    
    global cnx
    global cursor
    global msg_ids
    global poll_start

    i = 0
    for value in data_json.values():
        time.sleep(1)
        message = context.bot.send_poll(chat_id=value['chat_id'], 
                                        question=value['question'], 
                                        options=value['options'],
                                        is_anonymous=True, 
                                        allows_multiple_answers=False, 
                                        open_period=2592000)
        msg_ids[i] = message['message_id']
        poll_start = round(message['date'].timestamp())
        i += 1
        sql = 'INSERT INTO polls (id, chat_id, chat_name) VALUES (%s, %s, %s)'
        cursor.execute(sql, (message['poll']['id'], message['chat']['id'], message['chat']['title']))
        cnx.commit()



def get_answer(update, context):

    said_yes = update.poll.options[0]['voter_count']
    said_no = update.poll.options[1]['voter_count']
    total_voter_count = update.poll['total_voter_count']

    global cnx
    global cursor

	#CREATE TABLE poll_res (poll_id bigint(20) NOT NULL, question varchar(255), market(255), yes int(11), no int(11), voter_count int(11), poll_start timestamp, PRIMARY KEY (poll_id))
    
    cursor.execute('SELECT poll_id FROM poll_res WHERE poll_id = %s', (update.poll['id'],))
    result = cursor.fetchall()
    
    if result != []:
        sql = 'UPDATE poll_res SET yes=%s, no=%s, voter_count=%s WHERE poll_id = %s'
        cursor.execute(sql, (said_yes, said_no, total_voter_count, result[0][0]))
    else:
        poll_start =  datetime.now()
        sql = 'INSERT INTO poll_res (poll_id, question, yes, no, voter_count, poll_start) VALUES (%s, %s, %s, %s, %s, %s)'
        cursor.execute(sql, (update.poll['id'], update.poll['question'], said_yes, said_no, total_voter_count, poll_start))
    cnx.commit()



def regular_message(context: CallbackContext):

    f_json = open('file.json', 'r')
    data = f_json.read()
    data_json = json.loads(data)
    f_json.close()

    f_msg = open('message_file.txt', 'r')
    message_list = f_msg.read().split('\\v\n')
    f_msg.close()

    f = open('chat_full_list.txt', 'r')
    list = f.read().split()
    f.close()


    current_day = datetime.now()

    video = 'video/IMG_8202.MP4'

    video_2 = 'video/4754845845845.mp4'

    if current_day.hour == 11:

        context.bot.send_message(chat_id='-1001399858936', text=message_list[22])

    if current_day.weekday() == 0 and current_day.hour == 12:

        context.bot.send_message(chat_id='-1001390565200', text=message_list[19])
        context.bot.send_message(chat_id='-1001336819785', text=message_list[19])

    if current_day.hour == 20 and current_day.weekday() == 3:

        for chat in list:

            context.bot.send_message(chat_id=chat, text=message_list[2])
            context.bot.send_video(chat_id=chat, video=open(video, 'rb'), supports_streaming=True, width=720, height=1280)


    if current_day.weekday() == 3 and current_day.day % 2 == 0 and current_day.hour == 19:

        for chat in list:

            context.bot.send_video(chat_id=chat, video=open(video_2, 'rb'), supports_streaming=True, width=755, height=1070)


    if current_day.hour == 15:

        for value in data_json.values():

            j = 0

            if value['chat_id'] == '-1001288141021':
                j = 11

            if current_day.weekday() != 5:

                if current_day.weekday() != 2:
                    context.bot.send_message(chat_id=value['chat_id'], text=message_list[current_day.weekday() + j])

                else:
                    
                    context.bot.send_message(chat_id=value['chat_id'], text=message_list[current_day.weekday() + j])
                    context.bot.send_video(chat_id=value['chat_id'], video=open(video, 'rb'), supports_streaming=True, width=720, height=1280)

            else:

                photo = 'images/photo_saturday/photo_2020-12-03_15-41-25.jpg'
                context.bot.send_photo(chat_id=value['chat_id'], photo=open(photo, 'rb'))

    if current_day.hour == 18:

        context.bot.send_message(chat_id='@gigantandvkusvill1g', text=message_list[21])
        context.bot.send_message(chat_id='@gigantandvkusvill3g', text=message_list[21])



def regular_message_2(context: CallbackContext):

    f = open('chat_full_list.txt', 'r')
    list = f.read().split()
    f.close()

    f_msg = open('message_file.txt', 'r')
    message_list = f_msg.read().split('\\v\n')
    f_msg.close()

    current_day = datetime.now()

    if current_day.weekday() == 3:

        for chat in list:

            context.bot.send_message(chat_id=chat, text=message_list[20])




def quality_message(context: CallbackContext):
    f_msg = open('message_file.txt', 'r')
    message_list = f_msg.read().split('\\v\n')
    f_msg.close()
    
#    context.bot.send_message(chat_id='@gigantandvkusvill1g', text=message_list[7])
#    time.sleep(1)
#    context.bot.send_message(chat_id='@gigantandvkusvill3g', text=message_list[7])
# временно
    current_day = datetime.now()
    if current_day.hour == 12 or current_day.hour == 19:
        context.bot.send_message(chat_id='-1001399858936', text=message_list[10])
        context.bot.send_message(chat_id='-1001359408322', text=message_list[10])



def regular_photo(context: CallbackContext):
    f_json = open('file.json', 'r')
    data = f_json.read()
    data_json = json.loads(data)
    f_json.close()

    global photo_num
    current_day = datetime.now()
    photos = os.listdir(path='images/photos/')

    if current_day.weekday() == 4:
        for value in data_json.values():
    
            photo = 'images/photos/' + photos[photo_num]
            context.bot.send_photo(chat_id=value['chat_id'], photo=open(photo, 'rb'))
        photo_num = (photo_num + 1) % len(photos)



def regular_video(context: CallbackContext):
    f = open('chat_full_list.txt', 'r')
    list = f.read().split()
    f.close()

    f_msg = open('message_file.txt', 'r')
    message_list = f_msg.read().split('\\v\n')
    f_msg.close()

    current_day = datetime.now()

    if current_day.hour > 10:
        for i in list:
        
            video = 'video/t_video5206612213820820587.mp4'
            context.bot.send_video(chat_id=i, video=open(video, 'rb'), supports_streaming=True, caption=message_list[18], width=720, height=1280)


def regular_media_group(context: CallbackContext):
    
#    f_json = open('file.json', 'r')
#    data = f_json.read()
#    data_json = json.loads(data)
#    f_json.close()

    current_day = datetime.now()
    photos = os.listdir(path='images/media_group/')
    media = []

    for photo in photos:
        media.append(InputMediaPhoto(open('images/media_group/' + photo, 'rb')))

    if current_day.weekday() == 4:
        context.bot.send_media_group(chat_id='-1001287327432', media=media)
        context.bot.send_media_group(chat_id='-1001288141021', media=media)
    

def regular_forward_poll(context: CallbackContext):

    f_json = open('file.json', 'r')
    data = f_json.read()
    data_json = json.loads(data)
    f_json.close()

    global msg_ids
    global poll_start

    if (round(time.time()) - poll_start) < 120:
        return

    i = 0

    for value in data_json.values():

        context.bot.forward_message(chat_id=value['chat_id'], 
                                    message_id=msg_ids[i], 
                                    from_chat_id=value['chat_id'])
        i += 1



#job1 = j.run_repeating(regular_poll, interval=5184000, first=5)
job2 = j.run_repeating(regular_message, interval=3600, first=1)
job3 = j.run_repeating(regular_media_group, interval=86400, first=21600)
#job4 = j.run_repeating(regular_forward_poll, interval=30, first=0)
job5 = j.run_repeating(regular_photo, interval=86400, first=7200)
#job6 = j.run_repeating(quality_message, interval=3600, first=1)
#job7 = j.run_repeating(regular_message, interval=86400, first=1)
job8 = j.run_repeating(regular_video, interval=21600, first=14400)
job9 = j.run_repeating(regular_message_2, interval=86400, first=30600)
#updater.dispatcher.add_handler(CommandHandler('polls', job1, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('message', job2, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('media',job3, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('message2', job9, pass_job_queue=True))
#updater.dispatcher.add_handler(CommandHandler('forwarding_polls', job4, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('photo', job5, pass_job_queue=True))
#updater.dispatcher.add_handler(CommandHandler('quality', job6, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('video', job8, pass_job_queue=True))
#updater.dispatcher.add_handler(CommandHandler('message2', job7, pass_job_queue=True))
#updater.dispatcher.add_handler(PollHandler(get_answer, pass_chat_data=True))
updater.start_polling()
