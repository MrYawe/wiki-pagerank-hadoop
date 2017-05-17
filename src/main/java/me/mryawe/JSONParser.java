package me.mryawe;

import net.minidev.json.JSONObject;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by germain on 16/05/17.
 */
public class JSONParser {

    public static void exportToJSONFile(String txtFile, String output)
    {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(output));
        }
        catch(IOException ex) {
            ex.printStackTrace();
            return;
        }
        try(BufferedReader br = new BufferedReader(new FileReader(txtFile))) {
            String line = br.readLine();
            while (line != null) {
                String[] lineTabs = line.split("\t");
                if(lineTabs.length == 2) {
                    writer.write(elemToJsonObject(lineTabs[1], Double.parseDouble(lineTabs[0])).toJSONString());
                    writer.newLine();
                    writer.flush();
                }
                else
                {
                    System.out.println(lineTabs.toString());
                }
                line = br.readLine();
            }
            writer.close();
        }
        catch(Exception ex) {
            System.out.println("Error parsing to json" + ex.toString());
        }
    }


    private static JSONObject elemToJsonObject(String name, double pageRank)
    {
        JSONObject jo = new JSONObject();
        jo.put("name", name);
        jo.put("pagerank", pageRank);
        return jo;
    }
}
