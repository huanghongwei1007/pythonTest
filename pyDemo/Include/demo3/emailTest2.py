
# 循环发送邮件



import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import pymysql


def send_email_by_qq(to,sendtext):
    sender_mail = '770218284@qq.com'
    sender_pass = 'llrsohafpvnubdab'#同样是乱打的

    # 设置总的邮件体对象，对象类型为mixed
    msg_root = MIMEMultipart('mixed')
    # 邮件添加的头尾信息等
    msg_root['From'] = '770218284@qq.com<770218284@qq.com>'
    msg_root['To'] = to
    # 邮件的主题，显示在接收邮件的预览页面
    subject = 'python sendemail test successful'
    msg_root['subject'] = Header(subject, 'utf-8')

    # 构造文本内容
    text_info = sendtext
    text_sub = MIMEText(text_info, 'plain', 'utf-8')
    msg_root.attach(text_sub)


    try:
        sftp_obj =smtplib.SMTP('smtp.qq.com', 25)
        sftp_obj.login(sender_mail, sender_pass)
        sftp_obj.sendmail(sender_mail, to, msg_root.as_string())
        sftp_obj.quit()
        print('sendemail successful!')

    except Exception as e:
        print('sendemail failed next is the reason')
        print(e)

def select_db(sql):
    db = pymysql.connect(
        user = 'root',
        password = '9508Cjn',
        port = 3306,
        host = '106.15.228.35',
        db = 'PyThonTest',
        charset = 'utf8mb4'
    )
    cursor = db.cursor()
    cursor.execute(sql)
    numrows = int(cursor.rowcount)
    #to = '1057092126@qq.com'
    to = 'maggie.yan@nike.com'
    for index in range(10):
        jilu = cursor.fetchone()  ##此时 通过 jilu[0]
        print(jilu[0])
        send_email_by_qq(to,jilu[0])




if __name__ == '__main__':
    # 可以是一个列表，支持多个邮件地址同时发送，测试改成自己的邮箱地址
    #to = '1057092126@qq.com'
    #send_email_by_qq(to)
    #for index in range(1):
         #to = 'maggie.yan@nike.com'
         #send_email_by_qq(to,index)
    #print(strText(2))
    select_db("SELECT dicValue FROM hr_type_dictionary")











