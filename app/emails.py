from flask_mail import Mail, Message, Attachment
from app import app
mail = Mail(app)

def send_email(subject, sender, recipients, text_body, html_body):
	msg = Message(subject, sender=sender, recipients=recipients)
	msg.body = text_body
	msg.html = html_body
	mail.send(msg)

def send_attachment(subject, sender, recipients, text_body, html_body, filename, mimetype, file):
	msg = Message (subject, sender=sender, recipients=recipients)
	msg.body = text_body
	msg.html = html_body
	msg.attach(filename, mimetype, file.getvalue())
	mail.send(msg)
