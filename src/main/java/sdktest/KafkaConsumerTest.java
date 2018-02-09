package sdktest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import sdkwrapper.config.ConfigKeysIF;
import sdkwrapper.config.ConfigProperties;
import sdkwrapper.exceptions.ConfigurationException;
import sdkwrapper.exceptions.FabricRequestException;
import sdkwrapper.exceptions.FailedEndorsementException;
import sdkwrapper.exceptions.SDKWrapperInfrastructureException;
import sdkwrapper.runtimemgr.RuntimeMgrIF;
import sdkwrapper.service.FabricServices;
import sdkwrapper.vo.transaction.BlockTransactionAction;
import sdkwrapper.vo.transaction.FabricRequest;

public class KafkaConsumerTest
{
  private static final Logger logger = LogManager.getLogger( KafkaConsumerTest.class );

  private KafkaConsumer<String, String> consumer      = null;
  private Properties                    props         = null;
  private ConfigProperties              orgProps      = null; 
  private List<String>                  topics        = new ArrayList<String>();
  private FabricServices                fabricService = null;
  private String                        type          = null;
  
  @SuppressWarnings( "unused" )
  private KafkaConsumerTest()
  {
  }
  
  public KafkaConsumerTest( ConfigProperties config, String topic, String type )
   throws ConfigurationException
  {
    logger.info( "Starting KafkaConsumerTest." );

    this.orgProps = config;
    this.type     = type;
    topics.add( topic );
    
    props = new Properties();
    
    props.put( ConfigKeysIF.BOOTSTRAP_SERVERS,  orgProps.getProperty( ConfigKeysIF.BOOTSTRAP_SERVERS  ));
    props.put( ConfigKeysIF.CONSUMER_GROUP_ID,  orgProps.getProperty( ConfigKeysIF.CONSUMER_GROUP_ID  ));
    props.put( ConfigKeysIF.KEY_DESERIALIZER,   orgProps.getProperty( ConfigKeysIF.KEY_DESERIALIZER   ));
    props.put( ConfigKeysIF.VALUE_DESERIALIZER, orgProps.getProperty( ConfigKeysIF.VALUE_DESERIALIZER ));
    
    consumer = new KafkaConsumer<>(props);
    logger.info( "KafkaConsumer started in WrapperKafkaConsumer." );

    consumer.subscribe( topics );
    logger.info( "KafkaConsumer subscribed to topics list = " + topics );
  }
  
  public void consume()
  {
    logger.info( "Starting to consume messages." );
    
    while( true ) 
    {
      ConsumerRecords<String, String> records = consumer.poll(10);
        
      for( ConsumerRecord<String, String> record : records )
      {
        logger.info( "Received msg. Key = " + record.key() + ". Value = " + record.value() );

        if( "RESPONSE".compareTo(   type ) == 0 ) processResponse( record.value() );
        else if( "ERROR".compareTo( type ) == 0 ) processError( record.value() );
      }
      
      // update consumer offset
      consumer.commitSync();
    }
  }
  
  public void close()
  {
    if( consumer != null )
      consumer.close();
  }
  
  private void processResponse( String value )
  {
    BlockTransactionAction response = BlockTransactionAction.fromJSON( value );
    System.out.println( "Response Status = " + response.getResponseStatus() );
  }
  
  private void processError( String value )
  {
    System.out.println( "Error encountered = " + value );
  }
}
