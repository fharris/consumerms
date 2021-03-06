package com.example.democrudrep.configuration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.democrudrep.event.NewSentQueryPageEvent;
import com.example.democrudrep.event.SentQueryPageEvent;
import com.example.democrudrep.service.GameService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.Cursor;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails.Type;
import com.oracle.bmc.streaming.model.Message;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.http.ResteasyClientConfigurator;
import com.oracle.bmc.identity.IdentityClient;
import javax.ws.rs.client.ClientBuilder;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/insert2")
public class OcisdkApplication {
	final  Logger logger = LoggerFactory.getLogger(OcisdkApplication.class);

    @Value("${oci.config.stream.endpoint}")
	private  String streamEndpoint; 

    @Value("${oci.config.stream.id}")
    private  String streamId;

    @Value("${oci.config.compartment.id}")
	private String compartmentId; 
    
    public final  String UTF16 = "UTF-8";

    @Value("${email.username}")
	 private String username;

   private final GameService gameService;
    
   @GetMapping("/event/{userName}/{query}")
   Boolean insertEvent(@PathVariable String query, @PathVariable String userName){

   //Call OCI SDK
       
        log.info("inside /event/"+userName+"/"+query);
        System.out.println("reading value from propertes file using @value annotation");
	System.out.println("username ="+ username);
        System.out.println("streamEndpoint ="+ streamEndpoint);
        System.out.println("streamId ="+ streamId);
        SentQueryPageEvent sentQueryPageEvent = new SentQueryPageEvent(128L, 1L, "userName", "originalQuery", "query");
        gameService.newAttemptForUserEvents(sentQueryPageEvent);

     startConsuming();
       return true;
   }

        public void startConsuming() {
		
		logger.info("Starting App...");

		try {
                        consumeMessageFromStream(prepareOCICall());
		} catch (IOException e) {
			e.printStackTrace();
		}
        }


        public  StreamClient prepareOCICall() throws IOException {

                final InstancePrincipalsAuthenticationDetailsProvider provider;
  
                try {
                        logger.info("InstancePrincipalsAuthenticationDetailsProvider.builder()");
                        provider =
                                InstancePrincipalsAuthenticationDetailsProvider.builder()
                                        .additionalFederationClientConfigurator(
                                                new ResteasyClientConfigurator())
                                        .build();
                
                                        logger.info("Preparing OCI API clients (for Streaming)");		
                                             
                } catch (Exception e) {
                        if (e.getCause() instanceof SocketTimeoutException
                                || e.getCause() instanceof ConnectException) {
                        System.out.println(
                                "This sample only works when running on an OCI instance. Are you sure you???re running on an OCI instance? For more info see: https://docs.cloud.oracle.com/Content/Identity/Tasks/callingservicesfrominstances.htm");
                        return null;
                        }                   
                        throw e;
                }
   
                final IdentityClient identityClient =
                        IdentityClient.builder()
                                .additionalClientConfigurator(new ResteasyClientConfigurator())
                                .build(provider);

                // The following line is only necessary for this example because of our configuration in
                // resources/META-INF/services/javax.ws.rs.client.ClientBuilder
                // which enables Jersey by default. If you are using Resteasy by default, this line is not necessary
                System.setProperty(
                        ClientBuilder.JAXRS_DEFAULT_CLIENT_BUILDER_PROPERTY,
                        "org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder");

                                StreamClient streamClient =
                                StreamClient.builder()
                                .additionalClientConfigurator(new ResteasyClientConfigurator())
                                .endpoint(streamEndpoint)
                                .build(provider);
                                              
                return streamClient;
	}

        public void consumeMessageFromStream(StreamClient streamClient){
                logger.info("consumer starting...");
                int pollingCicle = 0;
                int messagesSize = 0;
                String cursor = createConsumerGroupCursor(streamClient).getValue(); // OCI API Call



                do {
                        logger.info("Polling Cycle {}",  pollingCicle);
                        
                        pollingCicle++;
                        
                        var getMessagesResponse = getMessages(streamClient, cursor); // OCI API Call
                        List<Message> messagesTotal = getMessagesResponse.getItems();

                        logger.info("Size of Total Messages: {}",  messagesTotal.size());
                        String nextCursor = getMessagesResponse.getOpcNextCursor();
                        List<Message> messages = getMessagesResponse.getItems();
                        for (Message msg : messages) {
                                logger.info("Size of Messages: {}",  messages.size());
                                messagesSize = messages.size();
                                logger.info("cursor {}",  nextCursor);
                        
                                String consumedMessage = "init";
                                try {
                                consumedMessage = new String(msg.getValue(), UTF16);
                                } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                                }
                                logger.info("Consumed message from the stream: {}", consumedMessage);
                                String json = consumedMessage;
                                ObjectMapper objectMapper = new ObjectMapper();
                                try {
                                //Car car = objectMapper.readValue(json, Car.class);
                                //logger.info("Car brand is {} and color is {}", car.getType(), car.getColor());

                                NewSentQueryPageEvent newSentQueryPageEvent = objectMapper.readValue(json, NewSentQueryPageEvent.class);
                                logger.info("Event Page Id {} and Event UserName {}", newSentQueryPageEvent.getQueryPageid(), newSentQueryPageEvent.getUserName());
                                
                                SentQueryPageEvent sentQueryPageEvent = new SentQueryPageEvent(newSentQueryPageEvent.getQueryPageid(), newSentQueryPageEvent.getUserId()
                                , newSentQueryPageEvent.getUserName()  ,  newSentQueryPageEvent.getOriginalQuery()   , newSentQueryPageEvent.getQuery());
                                gameService.newAttemptForUserEvents(sentQueryPageEvent);

                                } catch (JsonProcessingException e) {
                                e.printStackTrace();
                                }
                          
                        }
                        cursor = nextCursor;
                        logger.info("Size of Messages: {}",  messages.size());
                        try {
                                Thread.sleep(10* 1000);
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                } while ((pollingCicle < 50) & (messagesSize > 0));
        }

        private  Cursor createConsumerGroupCursor(StreamClient streamClient) {
      
                var createGroupCursorDetails = CreateGroupCursorDetails.builder()
                                                .groupName("all")
                                                .instanceName("consumerChampionship")
                                                .type(Type.Latest)
                                                .commitOnGet(true)
                                                .build();
                var createGroupCursorRequest = CreateGroupCursorRequest.builder()
                                                .streamId(streamId)
                                                .createGroupCursorDetails(createGroupCursorDetails)
                                                .build();
                var createGroupCursorResponse = streamClient.createGroupCursor(createGroupCursorRequest);
                int createGroupCursorResponseCode = createGroupCursorResponse.get__httpStatusCode__();
                if(createGroupCursorResponseCode != 200) {
                        logger.error("CreateGroupCursor failed - HTTP {}", createGroupCursorResponseCode);
                        System.exit(1);
                }
                return createGroupCursorResponse.getCursor();
        } 

        private  GetMessagesResponse getMessages(StreamClient streamClient, String cursor) {

                var getMessagesRequest = GetMessagesRequest.builder()
                                                .streamId(streamId)
                                                .cursor(cursor)
                                                .limit(10)
                                                .build();
                var getMessagesResponse = streamClient.getMessages(getMessagesRequest);
                int getMessagesResponseCode = getMessagesResponse.get__httpStatusCode__();
                if(getMessagesResponseCode != 200) {
                        logger.error("GetMessages failed - HTTP {}", getMessagesResponseCode);
                        System.exit(1);
                }
                return getMessagesResponse;
        }

}
