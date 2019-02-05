package app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Engine implements Runnable {

    private Connection connection;

    public Engine(Connection connection) {
        this.connection = connection;
    }

    public void run() {
        try {
            //this.getVillainsName();
            //this.getMinionNames();
            //this.addMinion();
            this.changeTownNamesCasing();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Task 1 - Get Villains’ Names
    private void getVillainsName() throws SQLException {
        String query =
                "SELECT villains.name, COUNT(minion_id) as cnt\n" +
                "FROM villains\n" +
                "JOIN minions_villains mv ON villains.id = mv.villain_id\n" +
                "GROUP BY villains.id\n" +
                "HAVING cnt > ?\n" +
                "ORDER BY cnt DESC\n";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);

        preparedStatement.setInt(1, 15);

        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()) {
            System.out.println(resultSet.getString(1) + " " + resultSet.getInt(2));
        }
    }

    // Task 2 - Get Minion Names
    private void getMinionNames() throws SQLException {
        String query =
                "SELECT minions.name, minions.age\n" +
                "FROM minions\n" +
                "JOIN minions_villains mv on minions.id = mv.minion_id\n" +
                "WHERE mv.villain_id = ?";

        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setInt(1,10);
        ResultSet resultSet = preparedStatement.executeQuery();

        if(!resultSet.next()) {
            System.out.println("No villain with this ID exists in the database.");
        } else {
            resultSet.beforeFirst();
        }

        while(resultSet.next()) {
            System.out.println(resultSet.getString(1) + " " + resultSet.getInt(2));
        }
    }

    // Task 3 - Add Minion
    private void addMinion() throws IOException, SQLException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        String minionArgs[] = bufferedReader.readLine().split(" ");
        String villainArgs[] = bufferedReader.readLine().split(" ");

        String minionName = minionArgs[1];
        int minionAge = Integer.parseInt(minionArgs[2]);
        String minionTown = minionArgs[3];

        String villainName = villainArgs[1];

        if(!this.isTownInDatabase(minionTown)) {
            this.insertTownIntoDatabase(minionTown);
            System.out.println("Town " + minionTown + " was added to the database.");
        }

        if(!this.isVillainInDatabase(villainName)) {
            this.insertVillainIntoDatabase(villainName);
            System.out.println("Villain " + villainName + " was added to the database.");
        }

        int villainID = this.getID(villainName, "villains");
        int townID = this.getID(minionTown, "towns");

        this.addMinionIntoDatabase(villainID, townID, minionName, minionAge);
        System.out.println("Successfully added " + minionName + " to be minion of " + villainName);

    }

    private boolean isTownInDatabase(String town) throws SQLException {
        String query =
                "SELECT *\n" +
                "FROM towns\n" +
                "WHERE name = ?";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, town);

        ResultSet resultSet = preparedStatement.executeQuery();

        return resultSet.next();
    }

    private void insertTownIntoDatabase(String minionTown) throws SQLException {
        String query =
                "INSERT INTO towns(name, country)\n" +
                "VALUES (? , NULL)";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, minionTown);

        preparedStatement.execute();
    }

    private boolean isVillainInDatabase(String villainName) throws SQLException {
        String query =
                "SELECT *\n" +
                "FROM villains\n" +
                "WHERE name = ?";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, villainName);
        ResultSet resultSet = preparedStatement.executeQuery();

        return resultSet.next();
    }

    private void insertVillainIntoDatabase(String villainName) throws SQLException {
        String query =
                "INSERT INTO villains(name)\n" +
                "VALUES (?)";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, villainName);

        preparedStatement.execute();
    }

    private int getID(String name, String table) throws SQLException {
        String query =
                        "SELECT " + table + ".id\n" +
                        "FROM " + table + "\n" +
                        "WHERE name = ?";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, name);

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();

        return resultSet.getInt(1);
    }

    private void addMinionIntoDatabase(int villainID, int townID, String minionName, int minionAge)
            throws SQLException {
        String query =
                "INSERT INTO minions(name, age, town_id)\n" +
                "VALUES (?, ?, ?)";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, minionName);
        preparedStatement.setInt(2, minionAge);
        preparedStatement.setInt(3, townID);

        preparedStatement.execute();

        int minionID = this.getID(minionName, "minions");
        this.addIntoManyToMany(minionID, villainID);

    }

    private void addIntoManyToMany(int minionID, int villainID) throws SQLException {
        String query =
                "INSERT INTO minions_villains(minion_id, villain_id)\n" +
                "VALUES (?, ?)";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setInt(1, minionID);
        preparedStatement.setInt(2, villainID);

        preparedStatement.execute();
    }

    // Task 4 - Change Town Names Casing
    private void changeTownNamesCasing() throws IOException, SQLException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        String country = bufferedReader.readLine();

        String query =
                "UPDATE towns\n" +
                "SET name = UPPER(name)\n" +
                "WHERE country = ?";
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        preparedStatement.setString(1, country);
        preparedStatement.execute();

        String querySelect =
                "SELECT name\n" +
                "FROM towns\n" +
                "WHERE country = ?";

        preparedStatement = this.connection.prepareStatement(querySelect);
        preparedStatement.setString(1, country);
        ResultSet resultSet = preparedStatement.executeQuery();

        if(!resultSet.next()) {
            System.out.println("No town names were affected.");
            return;
        }

        List<String> towns = new ArrayList<String>();
        resultSet.beforeFirst();
        while(resultSet.next()) {
            towns.add(resultSet.getString(1));
        }
        System.out.println(towns.size() + " town names were affected.");
        System.out.print("[");
        for(int i = 0; i < towns.size() - 1; i++) {
            System.out.printf("%s, ", towns.get(i));
        }
        System.out.printf("%s]", towns.get(towns.size() - 1));
    }
}
