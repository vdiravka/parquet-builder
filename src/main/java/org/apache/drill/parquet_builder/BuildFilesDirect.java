package org.apache.drill.parquet_builder;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class BuildFilesDirect
{
  private File destDir;

  public BuildFilesDirect( File destDir ) {
    this.destDir = destDir;
  }
  
  public void build( ) throws IOException {
    buildInt32( );
    buildInt32Int32( );
    buildInt2Date( );
    buildInt32Int16( );
    buildInt32Int8( );
    buildInt32Uint8( );
    buildInt32Uint16( );
    buildInt32Uint32( );
    buildOptionalMapRequiredBinaryRequiredInt64( );
  }

  /**
   * Builds a file using only the int32 storage type with no logical
   * <p>
   * type annotations.
   * 
   * @throws IOException
   */
  
  public void buildInt32() throws IOException {
    File outFile = new File( destDir, "int32.parquet" );
    String schemaText = "message int32Data { required int32 index; required int32 value; }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Integer.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Integer.MAX_VALUE ) );
    writer.close( );
  }

  /**
   * Builds a file using the int32 storage type plus the int_32 logical type.
   * <p>
   * This file is not valid in Drill.
   * 
   * @throws IOException
   */
  
  public void buildInt32Int32() throws IOException {
    File outFile = new File( destDir, "int_32.parquet" );
    String schemaText = "message int32Data { required int32 index; required int32 value (INT_32); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Integer.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Integer.MAX_VALUE ) );
    writer.close( );
  }
  
  /**
   * Builds a file using the int32 storage type plus the DATE logical type.
   * <p>
   * Drill accepts the file, but the resulting date values are off by about
   * 10,000 years.
   * 
   * @throws IOException
   */

  public void buildInt2Date() throws IOException {
    System.out.println( destDir.toString() );
    File outFile = new File( destDir, "date.parquet" );
    String schemaText = "message test { required int32 index; required int32 value (DATE); required int32 raw; }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Integer.MIN_VALUE ), new IntWritable( Integer.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Integer.MAX_VALUE ), new IntWritable( Integer.MAX_VALUE ) );
    int today = (int) (System.currentTimeMillis() / (24L*60*60*1000) );
    System.out.println( "Today: " + today );
    System.out.println( "Zero: " + (new Date( 0 ) ) );
    writer.write( new IntWritable( 6 ), new IntWritable( today ), new IntWritable( today ) );
    writer.close( );
  }
  
  /**
   * Builds a file with the int32 storage type and int_16 logical type.
   * <p>
   * Drill does not accept the file.
   * 
   * @throws IOException
   */
  
  public void buildInt32Int16() throws IOException {
    File outFile = new File( destDir, "int_16.parquet" );
    String schemaText = "message int16Data { required int32 index; required int32 value (INT_16); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Short.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Short.MAX_VALUE ) );
    writer.close( );
  }
  
  /**
   * Builds a file with the int32 storage type and int_8 logical type.
   * <p>
   * Drill does not accept the file.
   * 
   * @throws IOException
   */
  
  public void buildInt32Int8() throws IOException {
    File outFile = new File( destDir, "int_8.parquet" );
    String schemaText = "message int8Data { required int32 index; required int32 value (INT_8); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( Byte.MIN_VALUE ) );
    writer.write( new IntWritable( 5 ), new IntWritable( Byte.MAX_VALUE ) );
    writer.close( );
  }

  /**
   * Builds a file with the int32 storage type and uint_8 logical type.
   * <p>
   * Drill does not accept the file.
   * 
   * @throws IOException
   */
  
  public void buildInt32Uint8() throws IOException {
    File outFile = new File( destDir, "uint_8.parquet" );
    String schemaText = "message uint8Data { required int32 index; required int32 value (UINT_8); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( 0xFF ) );
    writer.close( );
  }

  /**
   * Builds a file with the int32 storage type and uint_16 logical type.
   * <p>
   * Drill does not accept the file.
   * 
   * @throws IOException
   */
  
  public void buildInt32Uint16() throws IOException {
    File outFile = new File( destDir, "uint_16.parquet" );
    String schemaText = "message uint16Data { required int32 index; required int32 value (UINT_16); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( 0xFFFFFF ) );
    writer.close( );
  }

  /**
   * Builds a file with the int32 storage type and uint_16 logical type.
   * <p>
   * Drill does not accept the file.
   * 
   * @throws IOException
   */
  
  public void buildInt32Uint32() throws IOException {
    File outFile = new File( destDir, "uint_32.parquet" );
    String schemaText = "message uint32Data { required int32 index; required int32 value (UINT_32); }";
    SimpleParquetWriter writer = new SimpleParquetWriter( outFile, schemaText );
    writer.write( new IntWritable( 1 ), new IntWritable( 0 ) );
    writer.write( new IntWritable( 2 ), new IntWritable( -1 ) );
    writer.write( new IntWritable( 3 ), new IntWritable( 1 ) );
    writer.write( new IntWritable( 4 ), new IntWritable( 0xFFFFFFFF ) );
    writer.close( );
  }

  /**
   * Builds a file with the map optional group type, repeated inner map group (with old type of annotation <br>
   * (MAP_KEY_VALUE) with key/value pairs (required binary UTF8 key and required int64 value).
   * <p>
   * Drill can read such file.
   *
   * @throws IOException
   */

  public void buildOptionalMapRequiredBinaryRequiredInt64() throws IOException {
    Path outFile = new Path(destDir.getPath(), "nested_types.parquet");

    final String schemaText =
        "message nested_map {" +
        " optional group map_field (MAP) {" +
        "  repeated group map (MAP_KEY_VALUE) {" +
        "   required binary key (UTF8);" +
        "   required int64 value; " +
        "  }" +
        " } " +
        "}";
    Configuration conf = new Configuration();
    MessageType schema = MessageTypeParser.parseMessageType(schemaText);
    GroupWriteSupport.setSchema(schema, conf);

    // ExampleParquetWriter is an example of ParquetWriter which can be used for creating nested (group) types
    ParquetWriter<Group> writer = ExampleParquetWriter
        .builder(outFile)
        .withType(schema)
        .withConf(conf)
        .build();

    SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
    Group group1 = groupFactory.newGroup();
    Group group2 = groupFactory.newGroup();
    Group group3 = groupFactory.newGroup();
    Group nullableGroup = groupFactory.newGroup();
    writer.write(nullableGroup);
    group1.addGroup("map_field")
        .addGroup("map")
        .append("key", "Google")
        .append("value", 1L);
    writer.write(group1);
    writer.write(nullableGroup);
    group2.addGroup("map_field")
        .addGroup("map")
        .append("key", "WhatsApp")
        .append("value", 2L);
    writer.write(group2);
    writer.write(nullableGroup);
    Group nestedGroup = group3.addGroup("map_field");
    nestedGroup
        .addGroup("map")
        .append("key", "Facebook")
        .append("value", 3L);
    nestedGroup
        .addGroup("map")
        .append("key", "Apple")
        .append("value", 4L);
    writer.write(group3);
    writer.close();
  }

}
