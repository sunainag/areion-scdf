package com.sproutloud.starter.stream.util;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Helper class to read CSV.
 * 
 * @author mgande
 *
 */
@Component
public class FileHelper {

    /**
     * Provides a Csv reader with given field separator and skips lines, if required (used if required to skip header while reading) for the given
     * file.
     *
     * @param in:     file to be read
     * @param isSkip: if header exists in the file, and needs to be skipped
     * @return : CSVReader object
     */
    public CSVReader getCsvReader(InputStream in, int isSkip) {
        Reader reader = new BufferedReader(new InputStreamReader(in));
        CSVParser parser = new CSVParserBuilder().withEscapeChar(ICSVParser.DEFAULT_ESCAPE_CHARACTER).withIgnoreLeadingWhiteSpace(true)
                .withSeparator(',').build();
        return new CSVReaderBuilder(reader).withSkipLines(isSkip).withCSVParser(parser).build();
    }

}
