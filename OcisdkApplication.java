package com.example.democrudrep.configuration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.democrudrep.domain.Car;
import com.example.democrudrep.event.NewSentQueryPageEvent;
import com.example.democrudrep.event.SentQueryPageEvent;
import com.example.democrudrep.service.GameService;
import com.example.democrudrep.service.InsertService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;



import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.Cursor;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails.Type;
import com.oracle.bmc.streaming.model.Message;
import org.springframework.context.ApplicationContext;
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
import com.oracle.bmc.identity.model.CreateUserDetails;
import com.oracle.bmc.identity.model.UpdateUserDetails;
import com.oracle.bmc.identity.requests.CreateUserRequest;
import com.oracle.bmc.identity.requests.DeleteUserRequest;
import com.oracle.bmc.identity.requests.GetUserRequest;
import com.oracle.bmc.identity.requests.UpdateUserRequest;
import com.oracle.bmc.identity.responses.CreateUserResponse;
import com.oracle.bmc.identity.responses.GetUserResponse;
import com.oracle.bmc.identity.responses.UpdateUserResponse;

import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageAsyncClient;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.Region;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.CreateBucketRequest;
import com.oracle.bmc.objectstorage.requests.DeleteBucketRequest;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.CreateBucketResponse;
import com.oracle.bmc.objectstorage.responses.DeleteBucketResponse;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.responses.AsyncHandler;

import javax.ws.rs.client.ClientBuilder;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;



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
                                "This sample only works when running on an OCI instance. Are you sure you’re running on an OCI instance? For more info see: https://docs.cloud.oracle.com/Content/Identity/Tasks/callingservicesfrominstances.htm");
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
                //getObjectSaveAsFile(osClient, osPathToObject); // OCI API Call
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
    // Create Consumer Group Cursor
    // https://docs.oracle.com/en-us/iaas/api/#/en/streaming/20180418/Cursor/CreateGroupCursor
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
    // GetMessages
    // https://docs.oracle.com/en-us/iaas/api/#/en/streaming/20180418/Message/GetMessages
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


private void instancePrincipalCode() {

    logger.info("inside instance principal code");

    // The following line is only necessary for this example because of our configuration in
    // resources/META-INF/services/javax.ws.rs.client.ClientBuilder
    // which enables Jersey by default. If you are using Resteasy by default, this line is not necessary   
    
    System.setProperty(
            ClientBuilder.JAXRS_DEFAULT_CLIENT_BUILDER_PROPERTY,
            "org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder");

    final InstancePrincipalsAuthenticationDetailsProvider provider;

    try {
        logger.info("InstancePrincipalsAuthenticationDetailsProvider.builder()");
        provider =
                InstancePrincipalsAuthenticationDetailsProvider.builder()
                        .additionalFederationClientConfigurator(
                                new ResteasyClientConfigurator())
                        .build();
    } catch (Exception e) {
        if (e.getCause() instanceof SocketTimeoutException
                || e.getCause() instanceof ConnectException) {
            System.out.println(
                    "This sample only works when running on an OCI instance. Are you sure you’re running on an OCI instance? For more info see: https://docs.cloud.oracle.com/Content/Identity/Tasks/callingservicesfrominstances.htm");
            return;
        }
        throw e;
    }

    final IdentityClient identityClient =
            IdentityClient.builder()
                    .additionalClientConfigurator(new ResteasyClientConfigurator())
                    .build(provider);

    // TODO: Pass in the compartment ID as an argument, or enter the value directly here (if known)
    final String compartmentId = "ocid1.tenancy.oc1..aaaaaaaa4wptnxymnypvjjltnejidchjhz6uimlhru7rdi5qb6qlnmrtgu3a";//params_in[0];
    System.out.println(compartmentId);

	
} //end method


private void instancePrincipalCode2() throws Exception {
    logger.info("inside instance principal 2 code");

    final String compartmentId = "ocid1.compartment.oc1..aaaaaaaamvevgnqfzs3o7ylphafotmwkhx6pst7mto2nl72rygd7wsa5hbzq";

    // Configuring the AuthenticationDetailsProvider. It's assuming there is a default OCI config file
    // "~/.oci/config", and a profile in that config with the name "DEFAULT". Make changes to the following
    // line if needed and use ConfigFileReader.parse(CONFIG_LOCATION, CONFIG_PROFILE);

    final InstancePrincipalsAuthenticationDetailsProvider provider;

    try {
        logger.info("InstancePrincipalsAuthenticationDetailsProvider.builder()");
        provider =
                InstancePrincipalsAuthenticationDetailsProvider.builder()
                        .additionalFederationClientConfigurator(
                                new ResteasyClientConfigurator())
                        .build();

                        logger.info("Preparing OCI API clients (for Streaming)");		
                        var streamClient = StreamClient.builder().endpoint(streamEndpoint).build(provider);

    } catch (Exception e) {
        if (e.getCause() instanceof SocketTimeoutException
                || e.getCause() instanceof ConnectException) {
            System.out.println(
                    "This sample only works when running on an OCI instance. Are you sure you’re running on an OCI instance? For more info see: https://docs.cloud.oracle.com/Content/Identity/Tasks/callingservicesfrominstances.htm");
            return;
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

    ObjectStorage objectStorageClient =
            ObjectStorageClient.builder()
                    .additionalClientConfigurator(new ResteasyClientConfigurator())
                    .build(provider);

    ObjectStorageAsyncClient objectStorageAsyncClient =
            ObjectStorageAsyncClient.builder()
                    .additionalClientConfigurator(new ResteasyClientConfigurator())
                    .build(provider);

    objectStorageClient.setRegion(Region.EU_FRANKFURT_1);
    objectStorageAsyncClient.setRegion(Region.EU_FRANKFURT_1);

    try {
        // region use sync api
        String namespace =
                objectStorageClient
                        .getNamespace(GetNamespaceRequest.builder().build())
                        .getValue();
        System.out.println("Using namespace: " + namespace);

        final String bucketName = "ResteasyClientBucket";
        final String objectName = "ResteasyClientObject";

        CreateBucketRequest createBucketRequest =
                CreateBucketRequest.builder()
                        .namespaceName(namespace)
                        .createBucketDetails(
                                CreateBucketDetails.builder()
                                        .compartmentId(compartmentId)
                                        .name(bucketName)
                                        .build())
                        .build();

        CreateBucketResponse createBucketResponse =
                objectStorageClient.createBucket(createBucketRequest);
        System.out.println("New bucket location: " + createBucketResponse.getLocation());

        final byte[] contents = "Hello World!".getBytes();
        InputStream stream = new ByteArrayInputStream(contents);

        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder()
                        .namespaceName(namespace)
                        .bucketName(bucketName)
                        .objectName(objectName)
                        .putObjectBody(stream)
                        .contentLength(Long.valueOf(contents.length))
                        .build();

        PutObjectResponse putObjectResponse = objectStorageClient.putObject(putObjectRequest);
        System.out.println("New object md5: " + putObjectResponse.getOpcContentMd5());

        GetObjectRequest getObjectRequest =
                GetObjectRequest.builder()
                        .namespaceName(namespace)
                        .bucketName(bucketName)
                        .objectName(objectName)
                        .build();

        GetObjectResponse getObjectResponse = objectStorageClient.getObject(getObjectRequest);
        System.out.println("Object md5: " + getObjectResponse.getContentMd5());
        getObjectResponse.getInputStream().close();

        DeleteObjectRequest deleteObjectRequest =
                DeleteObjectRequest.builder()
                        .namespaceName(namespace)
                        .bucketName(bucketName)
                        .objectName(objectName)
                        .build();

        objectStorageClient.deleteObject(deleteObjectRequest);

        DeleteBucketRequest deleteBucketRequest =
                DeleteBucketRequest.builder()
                        .namespaceName(namespace)
                        .bucketName(bucketName)
                        .build();

        objectStorageClient.deleteBucket(deleteBucketRequest);
        // endregion

        // region using Future
        Future<GetNamespaceResponse> getNamespaceResponseFuture =
                objectStorageAsyncClient.getNamespace(
                        GetNamespaceRequest.builder().build(), null);
        namespace = getNamespaceResponseFuture.get().getValue();
        System.out.println("Using namespace: " + namespace);

        Future<CreateBucketResponse> createBucketResponseFuture =
                objectStorageAsyncClient.createBucket(createBucketRequest, null);

        System.out.println(
                "New bucket location: " + createBucketResponseFuture.get().getLocation());

        stream.reset();
        Future<PutObjectResponse> putObjectResponseFuture =
                objectStorageAsyncClient.putObject(putObjectRequest, null);
        System.out.println(
                "New object md5: " + putObjectResponseFuture.get().getOpcContentMd5());

        Future<GetObjectResponse> getObjectResponseFuture =
                objectStorageAsyncClient.getObject(getObjectRequest, null);
        System.out.println("Object md5: " + getObjectResponseFuture.get().getContentMd5());
        getObjectResponseFuture.get().getInputStream().close();

        Future<DeleteObjectResponse> deleteObjectResponseFuture =
                objectStorageAsyncClient.deleteObject(deleteObjectRequest, null);
        deleteObjectResponseFuture.get();

        Future<DeleteBucketResponse> deleteBucketResponseFuture =
                objectStorageAsyncClient.deleteBucket(deleteBucketRequest, null);
        deleteBucketResponseFuture.get();
        // endregion

        // region use callback
        ResponseHandler<GetNamespaceRequest, GetNamespaceResponse> getNamespaceHandler =
                new ResponseHandler<>();
        objectStorageAsyncClient.getNamespace(
                GetNamespaceRequest.builder().build(), getNamespaceHandler);
        getNamespaceHandler.waitForCompletion();

        ResponseHandler<CreateBucketRequest, CreateBucketResponse> createBucketHandler =
                new ResponseHandler<>();
        objectStorageAsyncClient.createBucket(createBucketRequest, createBucketHandler);
        createBucketHandler.waitForCompletion();

        stream.reset();
        ResponseHandler<PutObjectRequest, PutObjectResponse> putObjectHandler =
                new ResponseHandler<>();
        objectStorageAsyncClient.putObject(putObjectRequest, putObjectHandler);
        putObjectHandler.waitForCompletion();

        ResponseHandler<GetObjectRequest, GetObjectResponse> getObjectHandler =
                new ResponseHandler<>();
        objectStorageAsyncClient.getObject(getObjectRequest, getObjectHandler);
        getObjectHandler.waitForCompletion();

        ResponseHandler<DeleteObjectRequest, DeleteObjectResponse> deleteObjectHandler =
                new ResponseHandler<>();
        objectStorageAsyncClient.deleteObject(deleteObjectRequest, deleteObjectHandler);
        deleteObjectHandler.waitForCompletion();

        ResponseHandler<DeleteBucketRequest, DeleteBucketResponse> deleteBucketHandler =
                new ResponseHandler<>();
        objectStorageAsyncClient.deleteBucket(deleteBucketRequest, deleteBucketHandler);
        deleteBucketHandler.waitForCompletion();
        // endregion
    } finally {
        objectStorageClient.close();
        objectStorageAsyncClient.close();
        System.clearProperty(ClientBuilder.JAXRS_DEFAULT_CLIENT_BUILDER_PROPERTY);
    }
}

static class ResponseHandler<IN, OUT> implements AsyncHandler<IN, OUT> {
    private Throwable failed = null;
    private CountDownLatch latch = new CountDownLatch(1);

    private void waitForCompletion() throws Exception {
        latch.await();
        if (failed != null) {
            if (failed instanceof Exception) {
                throw (Exception) failed;
            }
            throw (Error) failed;
        }
    }

    @Override
    public void onSuccess(IN request, OUT response) {
        if (response instanceof GetNamespaceResponse) {
            System.out.println(
                    "Using namespace: " + ((GetNamespaceResponse) response).getValue());
        } else if (response instanceof CreateBucketResponse) {
            System.out.println(
                    "New bucket location: " + ((CreateBucketResponse) response).getLocation());
        } else if (response instanceof PutObjectResponse) {
            System.out.println(
                    "New object md5: " + ((PutObjectResponse) response).getOpcContentMd5());
        } else if (response instanceof GetObjectResponse) {
            System.out.println("Object md5: " + ((GetObjectResponse) response).getContentMd5());
        }
        latch.countDown();
    }

    @Override
    public void onError(IN request, Throwable error) {
        failed = error;
        latch.countDown();
    }
}




} //end class


