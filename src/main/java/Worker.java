import com.sun.corba.se.spi.orbutil.threadpool.Work;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class Worker{
    int numOfTasksHandled = 0;
    int n;
    Sqs sqs;
    SqsClient sqsClient;
    String MANAGER_TO_WORKERS_QUEUE = "manager-to-workers-queue";
    String WORKERS_TO_MANAGER_QUEUE = "workers-to-manager-queue";
    Tesseract tess;

    public Worker(int n){
        this.n = n;
        this.sqsClient = SqsClient.create();
        this.sqs = new Sqs(sqsClient);
        tess = new Tesseract();
        tess.setDatapath("/usr/share/tesseract-ocr/4.00/tessdata");
    }

    public void start(){
        while(numOfTasksHandled < n){
            List<Message> messages = sqs.receiveOneMessageFromQueue(MANAGER_TO_WORKERS_QUEUE);
            for(Message message: messages){
                String[] dividedMessage = message.body().split("\n");
                String appName = dividedMessage[0].substring(4);
                String imgUrl = dividedMessage[1].substring(4);
                try{
                    URL url = new URL(imgUrl);
                    BufferedImage img = ImageIO.read(url);
                    String text = tess.doOCR(img);

                    String msg = "name:" + appName+"\n" + "imageUrl:" + imgUrl + "\n"+ "text:" + text;

                    sqs.sendMessageToQueue(WORKERS_TO_MANAGER_QUEUE, msg);

                }catch (TesseractException e){
                    System.out.println("Tesseract Exception : " + e.getMessage());
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                sqs.deleteMessageFromQueue(MANAGER_TO_WORKERS_QUEUE, message);
                numOfTasksHandled++;
            }
        }

        try {
            Runtime.getRuntime().exec("shutdown -s -t 3"); //shutdown after 5 seconds
        } catch (IOException e) {
            System.out.println("error - worker could not terminate");
        }

    }
}
