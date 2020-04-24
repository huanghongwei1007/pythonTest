#!/usr/bin/env/python3
# coding=utf-8
import base64
import csv
import json
import os
import pandas as pd
import datetime
import pymysql
import requests

today=datetime.date.today()

'''查询goal_mobile数据并邮件发送'''
def select_db(sql,file_name,field_name):
    db = pymysql.connect(
        user = '、',
        password = '、',
        port = 3306,
        host = '、',
        db = 'mobileapp',
        charset = 'utf8mb4'
    )
    cursor = db.cursor()
    cursor.execute(sql)
    numrows = int(cursor.rowcount)
    os.remove(file_name)
    with open(file_name,'a') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(field_name)
        for i in range(numrows):
            row = cursor.fetchone()
            writer.writerow(row)
 
def csv_xlsx():
    newdir = '/cust/export_data/csv'
    list = os.listdir(newdir)  
    writer = pd.ExcelWriter('report.xlsx')
    for i in range(len(list)):
        data = pd.read_csv('csv/' + list[i],encoding="utf-8",index_col=0)
        data.to_excel(writer, sheet_name=list[i])
        writer.save()


def sentmail():
    csv_file = open('report.xlsx','rb')
    base64_data = base64.b64encode(csv_file.read())
    base64_data = str(base64_data, 'utf-8')
    host = r'https://10.92.38.147/api/notification/productmaster'
    headers = {
        'Content-Type': "application/json"
    }

    body = {
        "recipients": "jason.hu@nike.com",
        "sendTo": "jason.hu@nike.com",
        "cc": "stone.pan@nike.com",
        "subject": " %s GOAL MOBILE  Data Report "%(today),
        "bodyText": "",
        "bodyHtml": "  This is %s Goal Mobile data report .Please check it. " %(today) ,
        "attachments": [{
            "filename": "Goal_mobile_report.xlsx",
            "filecontent": base64_data
        }]

    }

    body = json.dumps(body)
    print(body)
    r = requests.post(host, data = body, headers = headers, verify = False)
    print(r.content)

if __name__ == '__main__':
    select_db('select @rows:=@rows+1 as ID,message.* from(select hdr.id as hdrId,hm.id as memberId,hdr.create_time as RecordTime, hm.`name` as UserName,hm.mail as UserEmail,hm.type as UserType,hm.department as UserDepartment,hia.award_title as AwardName,"DoolMachine" as AwardSource from hr_draw_record hdr,hr_idp_award hia,hr_member hm where hdr.award_id=hia.id and hdr.member_id=hm.id UNION select 0 as hdrId,hm.id as memberId,hm.idp_status_update_time as RecordTime,hm.`name` as UserName,hm.mail as UserEmail,hm.type as UserType,hm.department as UserDepartment, "CoffeeCoupons" as AwardName,"IDP" as AwardSource from hr_member hm where hm.idp_status=1 and hm.training_survey_status=1) message,(select @rownum:=0,@gnum:=0,@rows:=0) number;','/cust/export_data/csv/awards.csv',('ID','hdrId','memberId','RecordTime','UserName','UserEmail','UserType','UserDepartment','AwardName','AwardSource'))
    select_db('select * from ( select @rows:=@rows+1 as ID,message.* from(select hm.id as memberId,hbp.id as hbpId,hbp.action_time as RecordTime,hm.`name` as UserName,hm.mail as UserEmail,hm.type as UserType,hbp.action_type as ActionType,hbp.target_module_id as TargetModuleID,hbp.target_page_id as TargetPageID,hbp.source_page_id as SourcePageID,hbp.source_button_id as SourceButtonID,hm.department as UserDepartment,hbp.device_name as DeviceName,hbp.client_type as ClientType from hr_buried_point hbp,hr_member hm where hbp.member_id = hm.id) message,(select @rownum:=0,@gnum:=0,@rows:=0) number ) a   where ID >=0 ;','/cust/export_data/csv/login.csv', ('ID','memberId','hbpId','RecordTime','UserName','UserEmail','UserType','ActionType','TargetModuleID','TargetPageID','SourcePageID','SourceButtonID','UserDepartment','DeviceName','ClientType'))
    select_db('select @rows:=@rows+1 as ID, message.* from( select hsc.id as openpositonReportId,hm.id as memberId,hsc.create_time as RecordTime,hm.name as SenderName,hm.mail as SenderEmail,hm.type as SenderType,hm.department as SenderDepartment, hsc.applicant_telephone_number as PhoneNumber,hsc.name_of_recommender as CandidateName,he.title as PositionName from hr_score_correlation hsc,hr_event he,hr_member hm where hsc.correlation_id=he.id and hsc.member_id =hm.id and hsc.score_type=1 ) message,(select @rownum:=0,@gnum:=0,@rows:=0) number;','/cust/export_data/csv/referral.csv', ('ID','openpositonReportId','memberId','RecordTime','SenderName','SenderEmail', 'SenderType','SenderDepartment','PhoneNumber','CandidateName','PositionName'))
    select_db('select @rows:=@rows+1 as ID, message.* from( select htc.id as thankYouCardId,hm1.id as sendId,hm2.id as receiverId,htc.create_time as RecordTime,hm1.name as SenderName,hm1.mail as SenderEmail,hm1.type as SenderType,hm1.department as SenderDepartment,  hm2.name as ReceiverName,hm2.mail as ReceiverEmail,hm2.type as ReceiverType,hm2.department as ReceiverDepartment,htc.content as CardContent from hr_thanks_card htc,hr_member hm1,hr_member hm2 where htc.from_member_id=hm1.id and htc.to_member_id=hm2.id ) message,(select @rownum:=0,@gnum:=0,@rows:=0) number;','/cust/export_data/csv/thanks.csv', ('ID','thankYouCardId','sendId','receiverId','RecordTime','SenderName','SenderEmail','SenderType','SenderDepartment','ReceiverName','ReceiverEmail','ReceiverType','ReceiverDepartment','CardContent'))
    select_db('select @rows:=@rows+1 as ID, message.* from( select hm.id as memberId,hm.`name` as UserName,hm.mail as UserEmail, hm.type as UserType,hm.department as UserDepartment,hmts.combined_score as UserScore,hm.score_level as UserLevel, if(((select count(id) from hr_buried_point where member_id=hm.id)>0) ,1,0) RegisteFlag, if((hm.idp_status=1 and hm.training_survey_status=1) ,1,0) as IDPGift, if((hm.idp_detail_status=1) ,1,0) as DoolMachineGift from hr_member hm LEFT JOIN hr_member_total_score hmts on hm.id=hmts.member_id ) message,(select @rownum:=0,@gnum:=0,@rows:=0) number;','/cust/export_data/csv/user.csv', ('ID','memberId','UserName','UserEmail','UserType','UserDepartment','UserScore','UserLevel','RegisteFlag','IDPGift','DoolMachineGift'))
    select_db('select @rows:=@rows+1 as ID, message.* from( select hsc.id as hscId,hm.id as memberId,hsc.create_time as RecordTime,hm.`name` as UserName,hm.mail as UserEmail, hm.type as UserType,hm.department as UserDepartment,hg.title as ActiveName, he.title as SubActiveName,if(hg.gatheringType=0,"training",if(hg.gatheringType=1,"activity","")) as gatheringType from hr_score_correlation hsc,hr_member hm,hr_event he,hr_gathering hg where hsc.correlation_id=he.id and he.gatheringId=hg.id and hsc.member_id=hm.id and hsc.score_type=0 ORDER BY hsc.create_time desc ) message,(select @rownum:=0,@gnum:=0,@rows:=0) number;','/cust/export_data/csv/sig.csv',('ID','hscId','memberId','RecordTime','UserName','UserEmail','UserType','UserDepartment','ActiveName','SubActiveName','gatheringType'))
    select_db('select @rows:=@rows+1 as ID, message.* from( select hm.id as memberId, hm.name as Name,hm.mail as Email,hm.type as userType,hm.department as userDepartment,hpq.question as question,hpa.answer,hpa.create_time from hr_pitch_question hpq LEFT JOIN hr_pitch_answer hpa on hpq.uuid=hpa.question_uuid LEFT JOIN hr_member hm on hm.id=hpa.member_id ) message,(select @rownum:=0,@gnum:=0,@rows:=0) number;','/cust/export_data/csv/innovation.csv', ('ID','memberId','Name','Email','userType','userDepartment','question','answer','create_time'))
    csv_xlsx()
    sentmail()
