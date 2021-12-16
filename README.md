# CSV to Parquet Converter

A java application to convert csv file to parquet file.

## Download Instruction:
Download the application from the following link: https://github.com/ColorlessCoder/csv-to-parquet-converter-java/blob/main/release/csv-to-parquet-converter.jar

## Minimum Requirement:
- Java Runtime Environment (JRE) or Java Development Kit (JDK) of version 6 or higher

## Usage
- Run the jar file
- Select a file via Open button
- Select the separator in csv file, default will be comma
- Data compression can be enabled or disabled, if enabled then parquet file will be compressed (size will be reduced) using dictionary.
- Finally, convert button will convert the csv to parquet file

## Development
- Clone the repo
- Open the project in an editor
- `mvn clean install`
- To package jar, `mvn package` and the jar file can be found in directory `target` with name `com.shakil.parquet.converter-1.0-SNAPSHOT-spring-boot.jar`