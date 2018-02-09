package sdktest;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import sdkwrapper.exceptions.FabricRequestException;


public class ExampleTwoVO
{
  private static final Logger logger = LogManager.getLogger( ExampleTwoVO.class );

  private String source = null;
  private String target = null;
  private int    amount = 0;
  
  public String getSource() { return source; }
  public String getTarget() { return target; }
  public int    getAmount() { return amount; }
  
  public void setSource( String source ) { this.source = source; }
  public void setTarget( String target ) { this.target = target; }
  public void setAmount( int    amount ) { this.amount = amount; }

  public static ExampleTwoVO fromJson( String value )
   throws FabricRequestException
  {
    Gson         gson     = new Gson();
    Type         ORG_TYPE = new TypeToken<ExampleTwoVO>() {}.getType();
    ExampleTwoVO vo       = null;
    
    logger.info( "ExampleTwoVO.fromJson received " + value );

    StringReader in     = new StringReader( value );
    JsonReader   reader = new JsonReader( in );
      
    vo = gson.fromJson( reader, ORG_TYPE );

    try
    {
      in.close();
      reader.close();
    } catch( IOException e )
      {
        e.printStackTrace();
      }
    
    if( vo == null )
      throw new FabricRequestException( "Unable to parse value." );

    logger.info( "ExampleTwoVO.fromJson Complete." );
    
    return vo;
  }
  
  public String[] toInvokeList()
  {
    return new String[] { source, target, new Integer( amount ).toString() };
  }
  
  public String[] toQueryList()
  {
    return new String[] { source };
  }
}
