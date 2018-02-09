package sdktest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;

import sdkwrapper.config.ConfigKeysIF;
import sdkwrapper.config.ConfigProperties;
import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.kafka.WrapperKafkaConsumer;
import sdkwrapper.kafka.WrapperKafkaProducer;
import sdkwrapper.runtimemgr.RuntimeMgrIF;
import sdkwrapper.vo.transaction.FabricRequest;

/**
 * From the e2e-cli example2 the pattern is:
 *   peer chaincode invoke -o orderer.example.com:7050 -C $CHANNEL_NAME -n mycc -c '{"Args":["invoke","a","b","10"]}' >&log.txt
 *   peer chaincode query -C $CHANNEL_NAME -n mycc -c '{"Args":["query","a"]}' >&log.txt
 *
 * @author tim
 *
 */
public class SdkTest
{
  private static final int SOURCE_ORG_NUM = 0;
  private static final int TARGET_ORG_NUM = 1;
  private static final int METHOD_NUM     = 2;
  private static final int SOURCE_NUM     = 3;
  private static final int TARGET_NUM     = 4;
  private static final int AMOUNT_NUM     = 5;
  
  private List<List<String>> cmds     = new ArrayList<List<String>>();
  private ConfigProperties   orgProps = null;
  private KafkaProducerTest  producer = null;
  private KafkaConsumerTest  responseConsumer = null;
  private KafkaConsumerTest  errorConsumer    = null;

  
  public SdkTest( String orgPath, String tranPath )
   throws ConfigurationException
  {
    loadOrgContextConfig( orgPath  );
 
    producer = new KafkaProducerTest( orgProps );
    responseConsumer = new KafkaConsumerTest( orgProps, orgProps.getProperty( ConfigKeysIF.RESPONSE_TOPIC ), "RESPONSE" );
    errorConsumer    = new KafkaConsumerTest( orgProps, orgProps.getProperty( ConfigKeysIF.ERROR_TOPIC    ), "ERROR"    );
 
    readCmdFile( tranPath );
    
    if( cmds == null || cmds.isEmpty() )
    {
      System.out.println( "No cmds found." );
      return;
    }
    
    for( List<String> elem: cmds )
    {
      processCmd( elem );
    }
    
    responseConsumer.consume();
    errorConsumer.consume();
  }
  
  private void processCmd( List<String> elem )
  {
    String method = elem.get( METHOD_NUM );
    
    if( method.equalsIgnoreCase( "invoke" ))
    {
      for( int i = 0; i < 4; i++ )
      {
      Future<RecordMetadata> result = processChgCmd( elem );
      while( !result.isDone() ) 
      {
        System.out.println( "Task is not completed yet...." );
        try
        {
          Thread.sleep(100);
        } catch( InterruptedException e )
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } //sleep for 1 millisecond before checking again
      }
      
      try
      {
        RecordMetadata data = result.get();
        System.out.println( "Result topic = " + data.topic() + "; Offset = " + data.offset() + "; Partition = " + data.partition() );
      } catch( InterruptedException | ExecutionException e )
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      }
    }
    else if( method.equalsIgnoreCase( "query" ))
    {
      Future<RecordMetadata> result = processQueryCmd( elem );
      while( !result.isDone() ) 
      {
        System.out.println( "Task is not completed yet...." );
        try
        {
          Thread.sleep(100);
        } catch( InterruptedException e )
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } //sleep for 1 millisecond before checking again
      }
      
      try
      {
        System.out.println( result.get().toString() );
      } catch( InterruptedException | ExecutionException e )
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  private Future<RecordMetadata> processChgCmd( List<String> elem )
  {
    Properties    props   = loadInvokeProps( elem );
    FabricRequest request = new FabricRequest();
    
    request.setRequestContext( "sdkwrapper.request.context.FabricRequestInvoke" );
    
    request.setContextProps( props );
    request.setSendDateTime( (new Date()).toString() );
    request.setTranPayload( loadInvokePayload( elem ));

    String topic = getTopic( elem );
    String key   = UUID.randomUUID().toString();
  
    Future<RecordMetadata> result = null;
    try
    {
      result = producer.send( topic, key, request.toJSON() );
    }
    catch( Exception e )
    {
      System.out.println( "Producer Exception encountered. Error = " + e.getMessage() );
    }
    
    return result;
  }
  
  private Future<RecordMetadata> processQueryCmd( List<String> elem )
  {
    Properties    props   = loadQueryProps( elem );
    FabricRequest request = new FabricRequest();
    
    request.setRequestContext( "sdkwrapper.request.context.FabricRequestInvoke" );
    
    request.setContextProps( props );
    request.setSendDateTime( (new Date()).toString() );
    request.setTranPayload( loadQueryPayload( elem ));
 
    String topic = getTopic( elem );
    String key   = UUID.randomUUID().toString();
  
    return producer.send( topic, key, request.toJSON() );
  }

  private Properties loadInvokeProps( List<String> elem )
  {
    String sourceOrg = elem.get( SOURCE_ORG_NUM );
    String targetOrg = elem.get( TARGET_ORG_NUM );
    String method    = elem.get( METHOD_NUM     );
    String source    = elem.get( SOURCE_NUM     );
    String target    = elem.get( TARGET_NUM     );
    String amount    = elem.get( AMOUNT_NUM     );

    Properties props = new Properties();
    props.put( "sourceOrg", sourceOrg );
    props.put( "targetOrg", targetOrg );
    props.put( "userId",    "User1"   );
    props.put( "method", method );
    props.put( "source", source );
    props.put( "target", target );
    props.put( "amount", amount );
    
    return props;
  }

  private Properties loadQueryProps( List<String> elem )
  {
    String sourceOrg = elem.get( SOURCE_ORG_NUM );
    String targetOrg = elem.get( TARGET_ORG_NUM );
    String method    = elem.get( METHOD_NUM     );
    String source    = elem.get( SOURCE_NUM     );

    Properties props = new Properties();
    props.put( "sourceOrg", sourceOrg );
    props.put( "targetOrg", targetOrg );
    props.put( "userId",    "User1"   );
    props.put( "method", method );
    props.put( "source", source );

    return props;
  }
 
  private String[] loadInvokePayload( List<String> elem )
  {
    ExampleTwoVO vo = new ExampleTwoVO();
    vo.setSource( elem.get( SOURCE_NUM ));
    vo.setTarget( elem.get( TARGET_NUM ));
    vo.setAmount( new Integer( elem.get( AMOUNT_NUM )));
    
    return vo.toInvokeList();
  }

  private String[] loadQueryPayload( List<String> elem )
  {
    ExampleTwoVO vo = new ExampleTwoVO();
    vo.setSource( elem.get( SOURCE_NUM ));
    
    return vo.toQueryList();
  }

  private void loadOrgContextConfig( String propsPath )
   throws ConfigurationException
  {
    orgProps = new ConfigProperties( propsPath ); 
  }

  private void readCmdFile( String tranPath )
  {
    BufferedReader br = null;
    
    try
    {
      br = new BufferedReader( new FileReader( tranPath ));
      String line;
   
      while(( line = br.readLine() ) != null ) 
      {
        List<String> elem = new ArrayList<String>(Arrays.asList( line.split( "," )));
        cmds.add( elem );
      }
    } 
    catch( IOException e ) 
    {
      System.out.println( e.getMessage() );  
    }
    finally
    {
      try
      {
        br.close();
      } 
      catch( IOException e )
      {
        e.printStackTrace();
      }
    }
  }

  private String getTopic( List<String> elems )
  {
    String src = elems.get( SOURCE_ORG_NUM );
    String tgt = elems.get( TARGET_ORG_NUM );
    
    if(( src.compareTo( "Org1" ) == 0 ) && tgt.compareTo( "Org2" ) == 0 ) return "org1tran";
    if(( src.compareTo( "Org1" ) == 0 ) && tgt.compareTo( "Org3" ) == 0 ) return "org3tran";
    if(( src.compareTo( "Org1" ) == 0 ) && tgt.compareTo( "Org4" ) == 0 ) return "org4tran";
    
    return "org2tran";
  }
  
  public static void main( String[] args ) 
   throws ConfigurationException
  {
    String orgPath  = null;
    String tranPath = null;

    if( args.length < 1 ) orgPath  = "resources/org-context.properties";
     else orgPath = args[0];
    
    if( args.length < 2 ) tranPath = "resources/tranCmds.txt";
     else tranPath = args[1];
  
    new SdkTest( orgPath, tranPath );
  }
  
}
