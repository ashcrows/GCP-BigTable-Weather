import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Bigtable {

    public final String projectId = "g24ai1067-vcc-assignment";
    public final String instanceId = "bda-assignment04";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather";

    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;

    public static void main(String[] args) {
        try {
            Bigtable bt = new Bigtable();
            bt.run();
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void run() throws Exception {
        System.out.println("Step 1: Connecting to Bigtable...");
        connect();
        System.out.println("Connection established successfully.\n");

        System.out.println("Step 2: Deleting table if exists...");
        deleteTable();
        System.out.println("Table deleted if it existed.\n");

        System.out.println("Step 3: Creating table...");
        createTable();
        System.out.println("Table created successfully.\n");

        System.out.println("Step 4: Loading sensor data into table...");
        loadData();
        System.out.println("Data loaded successfully.");
        showTableRowCount();

        System.out.println("\nStep 5: Query 1 - Temperature at Vancouver on 2022-10-01 10 AM...");
        int temp = query1();
        System.out.println("Temperature at Vancouver: " + temp + "\n");

        System.out.println("Step 6: Query 2 - Highest wind speed in Portland, September 2022...");
        int windspeed = query2();
        System.out.println("Highest Windspeed in Portland: " + windspeed + "\n");

        System.out.println("Step 7: Query 3 - All readings for SeaTac on 2022-10-02...");
        ArrayList<Object[]> data = query3();
        System.out.println("SeaTac Readings for 2022-10-02:");
        for (Object[] row : data) {
            System.out.println(Arrays.toString(row));
        }
        System.out.println("Total rows for SeaTac on 2022-10-02: " + data.size() + "\n");

        System.out.println("Step 8: Query 4 - Highest temperature at any station in summer 2022...");
        temp = query4();
        System.out.println("Highest Temperature in Summer 2022: " + temp + "\n");

        System.out.println("Step 9: Closing connection...");
        close();
        System.out.println("All operations completed.\n");
    }

    public void connect() throws Exception {
        dataClient = BigtableDataClient.create(projectId, instanceId);
        adminClient = BigtableTableAdminClient.create(projectId, instanceId);
    }

    public void createTable() {
        CreateTableRequest request = CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
        adminClient.createTable(request);
    }

    public void loadData() throws Exception {
        String[] stations = {"SEA", "YVR", "PDX"};
        String[] files = {"bin/data/seatac.csv", "bin/data/vancouver.csv", "bin/data/portland.csv"};

        for (int s = 0; s < stations.length; s++) {
            BufferedReader reader = new BufferedReader(new FileReader(files[s]));
            String line;
            Set<String> seenHours = new HashSet<>();
            BulkMutation bulkMutation = BulkMutation.create(tableId);

            reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                String date = parts[1];
                String time = parts[2];
                String hour = time.split(":")[0];
                String key = date + hour;
                if (seenHours.contains(key)) continue;
                seenHours.add(key);

                String rowKey = stations[s] + "#" + date + "#" + hour;
                RowMutationEntry mutation = RowMutationEntry.create(rowKey)
                        .setCell(COLUMN_FAMILY, "temperature", parts[3])
                        .setCell(COLUMN_FAMILY, "dewpoint", parts[4])
                        .setCell(COLUMN_FAMILY, "humidity", parts[5])
                        .setCell(COLUMN_FAMILY, "windspeed", parts[6])
                        .setCell(COLUMN_FAMILY, "pressure", parts[8]);
                bulkMutation.add(mutation);
            }
            reader.close();
            dataClient.bulkMutateRows(bulkMutation);
        }
    }

    public void showTableRowCount() {
        int count = 0;
        for (Row row : dataClient.readRows(Query.create(tableId))) {
            count++;
        }
        System.out.println("Total rows in table: " + count);
    }

    public int query1() {
        String rowKey = "YVR#2022-10-01#10";
        Row row = dataClient.readRow(tableId, rowKey);
        for (RowCell cell : row.getCells()) {
            if (cell.getQualifier().toStringUtf8().equals("temperature")) {
                return Integer.parseInt(cell.getValue().toStringUtf8());
            }
        }
        return 0;
    }

    public int query2() {
        int maxSpeed = 0;
        Query query = Query.create(tableId).prefix("PDX#2022-09");
        for (Row row : dataClient.readRows(query)) {
            for (RowCell cell : row.getCells()) {
                if (cell.getQualifier().toStringUtf8().equals("windspeed")) {
                    int speed = Integer.parseInt(cell.getValue().toStringUtf8());
                    if (speed > maxSpeed) maxSpeed = speed;
                }
            }
        }
        return maxSpeed;
    }

    public ArrayList<Object[]> query3() {
        ArrayList<Object[]> results = new ArrayList<>();
        Query query = Query.create(tableId).prefix("SEA#2022-10-02");
        for (Row row : dataClient.readRows(query)) {
            String[] keys = row.getKey().toStringUtf8().split("#");
            String date = keys[1];
            String hour = keys[2];
            int temp = 0, dew = 0;
            String hum = "", wind = "", pres = "";
            for (RowCell cell : row.getCells()) {
                switch (cell.getQualifier().toStringUtf8()) {
                    case "temperature": temp = Integer.parseInt(cell.getValue().toStringUtf8()); break;
                    case "dewpoint": dew = Integer.parseInt(cell.getValue().toStringUtf8()); break;
                    case "humidity": hum = cell.getValue().toStringUtf8(); break;
                    case "windspeed": wind = cell.getValue().toStringUtf8(); break;
                    case "pressure": pres = cell.getValue().toStringUtf8(); break;
                }
            }
            results.add(new Object[]{date, hour, temp, dew, hum, wind, pres});
        }
        return results;
    }

    public int query4() {
        int maxTemp = -100;
        String[] months = {"07", "08"};
        for (String month : months) {
            for (String station : new String[]{"SEA", "YVR", "PDX"}) {
                Query query = Query.create(tableId).prefix(station + "#2022-" + month);
                for (Row row : dataClient.readRows(query)) {
                    for (RowCell cell : row.getCells()) {
                        if (cell.getQualifier().toStringUtf8().equals("temperature")) {
                            int temp = Integer.parseInt(cell.getValue().toStringUtf8());
                            if (temp > maxTemp) maxTemp = temp;
                        }
                    }
                }
            }
        }
        return maxTemp;
    }

    public void deleteTable() {
        try {
            adminClient.deleteTable(tableId);
        } catch (NotFoundException e) {
        }
    }

    public void close() {
        dataClient.close();
        adminClient.close();
    }
}