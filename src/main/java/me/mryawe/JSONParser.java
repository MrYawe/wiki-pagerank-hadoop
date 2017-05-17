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
        ArrayList<JSONObject> res = new ArrayList<JSONObject>();
        try(BufferedReader br = new BufferedReader(new FileReader(txtFile))) {
            String line = br.readLine();
            while (line != null) {
                String[] lineTabs = line.split("\t");
                res.add(elemToJsonObject(lineTabs[1], Double.parseDouble(lineTabs[0])));
                line = br.readLine();
            }
        }
        catch(Exception ex)
        {
            System.out.println("Error reading file");
        }
        //Save json array
        BufferedWriter writer = null;
        try {

            writer = new BufferedWriter(new FileWriter(output));
            for ( int i = 0; i < res.size(); i++)
            {
                writer.write(res.get(i).toJSONString());
                writer.newLine();
                writer.flush();
            }
            writer.close();
        }
        catch(IOException ex) {
            ex.printStackTrace();
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
