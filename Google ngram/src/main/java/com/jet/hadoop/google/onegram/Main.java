package com.jet.hadoop.google.onegram;

import org.apache.hadoop.util.ToolRunner;

import com.jet.hadoop.google.onegram.avg_length.GoogleDataAverageWordLengthToolImpl;
import com.jet.hadoop.google.onegram.syllables.GoogleDataSyllablesToolImpl;
import com.jet.hadoop.google.onegram.wordcounter.GoogleDataWordCounterToolImpl;

/**
 * Title: Main.java<br>
 * Description: <br>
 * Created: 11-Apr-2016<br>
 * Copyright: Copyright (c) 2015<br>
 * @author Teja Sasank Gorthi (gteja231989@gmail.com)
 */
public class Main {

   public static void main(String[] args) throws Exception {
      int requested_job = Integer.parseInt(args[0]);
      switch(requested_job) {
      case 1:
            System.out.println("Loading the GoogleDataWordCounterToolImpl");
            int exitStatus = ToolRunner.run(new GoogleDataWordCounterToolImpl(), args);
            System.exit(exitStatus);
      case 2:
            System.out.println("Loading the GoogleDataAverageWordLengthToolImpl");
            exitStatus = ToolRunner.run(new GoogleDataAverageWordLengthToolImpl(), args);
            System.exit(exitStatus);
      case 3:
          System.out.println("Loading the GoogleDataSyllablesToolImpl");
          exitStatus = ToolRunner.run(new GoogleDataSyllablesToolImpl(), args);
          System.exit(exitStatus);
      case 4:
          System.out.println("Loading All the Tool Implementations");

          String[] arguments = new String[3];
          arguments[0] = args[0];
          arguments[1] = args[1];

          arguments[2] = args[2];
          exitStatus = ToolRunner.run(new GoogleDataWordCounterToolImpl(), arguments);

          arguments[2] = args[3];
          exitStatus += ToolRunner.run(new GoogleDataAverageWordLengthToolImpl(), arguments);

          arguments[2] = args[4];
          exitStatus += ToolRunner.run(new GoogleDataSyllablesToolImpl(), arguments);

          System.exit(exitStatus);
      default:
            throw new Exception("Invalid Job Request!");
      }

   }

}
