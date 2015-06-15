import java.util.*;
import javax.mail.*;
import javax.mail.Flags.Flag;
import javax.mail.search.FlagTerm;


public class speakRaspberry {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imaps");

        while(true)
        {
        try {
            Session session = Session.getInstance(props, null);
            Store store = session.getStore();
            store.connect("imap.gmail.com", "raspberrypivenkatsam@gmail.com", "*****");
            Folder inbox = store.getFolder("INBOX");
            inbox.open(Folder.READ_WRITE);
            Message[] msg = inbox.search(new FlagTerm(new Flags(Flag.SEEN), false));


      for (int i = 0, n = msg.length; i < n; i++) {
         Message message = msg[i];

         if(message.getFrom()[0].toString().contains("samvenkatram"))
         {

        try {
             Multipart mp = (Multipart) message.getContent();
            BodyPart bp = mp.getBodyPart(0);
 System.out.println(bp.getContent());
                Process p = Runtime.getRuntime().exec("sh speak.sh "+"\""+bp.getContent()+"\"");
            p.waitFor();
            p.destroy();
             message.setFlag(Flags.Flag.SEEN, true);
        } catch (Exception e) {

System.out.println("e");
}
         }
         }

      Thread.sleep(1000);
        } catch (Exception mex) {
            mex.printStackTrace();
        }

        }
    }
}
