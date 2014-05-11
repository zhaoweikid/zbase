# coding: utf-8
import os, sys, types
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from email.utils import COMMASPACE,formatdate
import smtplib
from zbase.base.logger import log

class MailMessage:
    def __init__(self, subject, fromaddr, toaddr, content):
        self.mailfrom = fromaddr
        self.mailto = toaddr

        self.msg = MIMEMultipart() 
        self.msg['From'] = fromaddr
        if type(subject) == types.UnicodeType:
            self.msg['Subject'] = '=?UTF-8?B?%s?=' % (base64.b64encode(subject.encode('utf-8')))
        else:
            self.msg['Subject'] = '=?UTF-8?B?%s?=' % (base64.b64encode(subject))

        if type(toaddr) in (types.TupleType, types.ListType):
            self.msg['To'] = COMMASPACE.join(toaddr)
        else:
            self.msg['To'] = toaddr
        self.msg['Date'] = formatdate(localtime=True) 
        if content.find('<') > 0 and content.find('>') > 0:
            self.msg.attach(MIMEText(content, 'html', 'utf-8')) 
        else:
            self.msg.attach(MIMEText(content, 'plain', 'utf-8')) 

    def append_file(self, filename, conttype):
        maintype, subtype = conttype.split('/')
        part = MIMEBase(maintype, subtype)
        part.set_payload(open(filename, 'rb').read())
        encoders.encode_base64(part) 
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(filename)) 
        self.msg.attach(part) 

    def append_data(self, content, conttype):
        maintype, subtype = conttype.split('/')
        part = MIMEBase(maintype, subtype)
        part.set_payload(content) 
        encoders.encode_base64(part) 
        self.msg.attach(part) 

    def tostring(self):
        return self.msg.as_string()


class MailSender:
    def __init__(self, server, username, password):
        self.smtpserver = server
        self.username   = username
        self.password   = password

    def send(self, msg):
        try:
            conn = smtplib.SMTP(self.smtpserver)
            conn.set_debuglevel(1)
            conn.login(self.username, self.password)
            conn.sendmail(msg.mailfrom, msg.mailto, msg.tostring())
            conn.quit()
            log.info('mail to:%s send succeed!', str(msg.mailto))
            return True
        except Exception, e:
            log.warn('mail to:%s send error! %s', str(msg.mailto), str(e))
            return False     


def test():
    m = MailMessage('test测试邮件', 'receipt@qfpay.net', 'zhaowei@qfpay.net', 'test content我们')
    m.append_file('sendmail.py', 'text/plain')
    print m.tostring()

    sender = MailSender('smtp.exmail.qq.com', 'receipt@qfpay.net', 'qianfang911')
    sender.send(m)


if __name__ == '__main__':
    test()
