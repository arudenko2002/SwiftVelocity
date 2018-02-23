package TrackAction;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class SSLEmail {
    public String sender = "swift.subscriptions@gmail.com";
    public String password = "gfsniwmiqxgjoxnl";
    private void sendJustMe(MimeMessage msg) throws Exception{
        msg.setRecipients(Message.RecipientType.TO,"alexey.rudenko@umusic.com,arudenko2002@yahoo.com");
    }
    private void sendGroup(MimeMessage msg) throws Exception{
        String recipients = "alexey.rudenko@umusic.com";
        recipients += ",arudenko2002@yahoo.com";
        recipients += ",Tom.Hovis@umusic.com";
        recipients += ",Justin.Roth@umusic.com";
        recipients += ",Srikanth.Komatireddy@umusic.com";
        recipients += ",Gevorg@umusic.com";
        recipients += ",KFK@umusic.com";
        msg.setRecipients(Message.RecipientType.TO, recipients);
    }

    public int sendMail(String to, String subject, String body, String whom) throws Exception {
        return sendMail("swift.subscriptions@gmail.com","gfsniwmiqxgjoxnl", to, subject, body, whom);
    }

    public int sendMail(String email,String app_password, String to, String subject, String body, String whom) throws Exception{
        final String  d_email = email,
                m_to = to,
                m_subject = subject,
                m_text = body,
                d_uname = email,
                d_password=app_password,
                d_host = "smtp.gmail.com",
                d_port  = "465";

        Properties props = new Properties();
        props.put("mail.smtp.user", d_email);
        props.put("mail.smtp.host", d_host);
        props.put("mail.smtp.port", d_port);
        props.put("mail.smtp.starttls.enable","true");
        props.put("mail.smtp.debug", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.socketFactory.port", d_port);
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", "false");

        Session session = Session.getInstance(props,
                new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(d_uname, d_password);
                    }
                });
        session.setDebug(true);
        MimeMessage msg = new MimeMessage(session);
        try {
            msg.setSubject(m_subject);
            msg.setFrom(new InternetAddress(d_email));
            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(m_to));
            if(whom.equals("justtome")) {
                sendJustMe(msg);
            }
            else if(whom.equals("toteam")) {
                sendGroup(msg);
            }
            else {}
            msg.setSubject(m_subject);
            msg.setContent(m_text,"text/html");

            Transport transport = session.getTransport("smtps");//transport.connect();
            transport.connect(d_host, Integer.valueOf(d_port), d_uname, d_password);
            transport.sendMessage(msg, msg.getAllRecipients());
            transport.close();

        } catch (AddressException e) {
            e.printStackTrace();
            return -1;
        } catch (MessagingException e) {
            e.printStackTrace();
            return -1;
        }
        System.out.println("Email sent: "+m_to);
        return 0;
    }

    public void sendUMGMail(String to, String subject, String body, String whom) throws Exception{
        Properties props = new Properties();
        props.put("mail.smtp.host", "smtphost.global.umusic.net");
        props.put("mail.smtp.port", 25);
        props.put("mail.smtp.socketFactory.fallback", "true");
        Session session = Session.getInstance(props);

        try {
            MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("noreply@umusic.com"));
            msg.setRecipients(Message.RecipientType.TO, to);
            if(whom.equals("justtome")) {
                sendJustMe(msg);
            }
            if(whom.equals("toteam")) {
                sendJustMe(msg);
            }
            //if(mail_to.startsWith("admin") || true) {
            //    sendJustMe(msg);
            //} else {
            //    msg.setRecipients(Message.RecipientType.TO, mail_to);
            //}
            msg.setSubject(subject);
            msg.setContent(body,"text/html");
            Transport transport = session.getTransport("smtp");
            transport.connect();
            transport.sendMessage(msg, msg.getAllRecipients());
            transport.close();
            System.out.println("Email sent!");
        } catch (AddressException e) {
            System.out.println(e.getMessage());
        } catch (MessagingException e) {
            System.out.println(e.getMessage());
        }
    }


    /**
     Outgoing Mail (SMTP) Server
     requires TLS or SSL: smtp.gmail.com (use authentication)
     Use Authentication: Yes
     Port for SSL: 465
     */
    public static void main(String[] args) throws Exception {
        SSLEmail ss = new SSLEmail();
        //ss.sendMail("arudenko2002@gmail.com","fxhcwbmwerkypvwd","alexey.rudenko@umusic.com","Probka arudenko2002@gmail.com","Probka is.");
        //ss.sendMail("swift.subscriptions@gmail.com","gfsniwmiqxgjoxnl","arudenko2002@yahoo.com","Probka swift@umusic.com","Probka is.");
        //ss.sendMail("alexey.rudenko2002@gmail.com","ugfepuhzgzfbybgj","arudenko2002@yahoo.com","Probka alexey.rudenko2002@gmail.com","Probka is.");
        //ss.sendMail2("alexey.rudenko@umusic.com","Welcome17","arudenko2002@yahoo.com","Probka alexey.rudenko2002@gmail.com","Probka is.");
        ss.sendUMGMail("","Subject #6","<h1>This is actual message</h1>","justtome");
    }
}
