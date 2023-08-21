import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
public class JunitTest {
    @Test
    public void test_i_Select() throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        // Create the movieStar table
        var movieStar = new Table ("movieStar", "name address gender birthdate",
                                    "String String Character String", "name");

        var star0 = new Comparable [] { "Carrie_Fisher", "Hollywood", 'F', "9/9/99" };
        var star1 = new Comparable [] { "Mark_Hamill", "Brentwood", 'M', "8/8/88" };
        var star2 = new Comparable [] { "Harrison_Ford", "Beverly_Hills", 'M', "7/7/77" };
        
        movieStar.insert (star0);
        movieStar.insert (star1);
        movieStar.insert (star2);
        movieStar.print ();

        // Create the expected table with only "name" equals to "Harrison_Ford"
        var expectedTable = new Table("expected movieStar i_select", "name address gender birthdate", "String String Character String", "name");
        // Insert stars into expectedTable
        expectedTable.insert(star2);
        
        // Perform the movieStar table select operation
        var result = movieStar.select (t -> t[movieStar.col("name")].equals ("Harrison_Ford"));
        
        // Get access to the "tuples" field using reflection
        Field tuplesField = Table.class.getDeclaredField("tuples");
        tuplesField.setAccessible(true);

        // Get the tuples list from the result and expected tables
        var resultTuples = (ArrayList<Comparable[]>) tuplesField.get(result);
        var expectedTuples = (ArrayList<Comparable[]>) tuplesField.get(expectedTable);
        
        // Verify the result is the same as expected
        assertEquals(expectedTuples.size(), resultTuples.size());
        for (int i = 0; i < expectedTuples.size(); i++) {
            assertArrayEquals(expectedTuples.get(i), resultTuples.get(i));
        }
    }

    @Test
    public void test_i_Join() throws NoSuchFieldException, IllegalAccessException {
        // Create the movie table
        var movie = new Table("movie", "title year length genre studioName producerNo",
                "String Integer Integer String String Integer", "title year");

        // Create the cinema table
        // var cinema = new Table("cinema", "title year length genre studioName producerNo",
        //         "String Integer Integer String String Integer", "title year");
        
        // Create the studio table
        var studio = new Table ("studio", "name address presNo",
                "String String Integer", "name");
        
        // movie table values
        var film0 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        var film1 = new Comparable [] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
        var film4 = new Comparable [] { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };

        // studio table
        var studio0 = new Comparable [] { "Fox", "Los_Angeles", 7777 };
        var studio1 = new Comparable [] { "Universal", "Universal_City", 8888 };
        var studio2 = new Comparable [] { "DreamWorks", "Universal_City", 9999 };

        // Add films to movie table
        movie.insert (film0);
        movie.insert (film1);
        movie.insert (film2);
        movie.insert (film3);

        // Add values to studio table
        studio.insert (studio0);
        studio.insert (studio1);
        studio.insert (studio2);
        
        // Create the expected table with joined columns
        var expectedTable = new Table("movie", "title year length genre studioName producerNo name address presNo",
                "String Integer Integer String String Integer String Integer", "name title year");

        // Insert data into expectedTable with joined columns
        var expectedRow0 = new Comparable[]{"Star_Wars", 1977, 124, "sciFi", "Fox", 12345, "Fox", "Los_Angeles", 7777};
        var expectedRow1 = new Comparable[]{"Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345, "Fox","Los_Angeles", 7777};
        var expectedRow2 = new Comparable[]{"Rocky", 1985, 200, "action", "Universal", 12125, "Universal","Universal_City", 8888};
        var expectedRow3 = new Comparable[]{"Rambo", 1978, 100, "action", "Universal", 32355, "Universal","Universal_City", 8888};

        expectedTable.insert(expectedRow0);
        expectedTable.insert(expectedRow1);
        expectedTable.insert(expectedRow2);
        expectedTable.insert(expectedRow3);

        // Perform the join operation
        var result = movie.join("studioName", "name", studio);

        // Get access to the "tuples" field using reflection
        Field tuplesField = Table.class.getDeclaredField("tuples");
        tuplesField.setAccessible(true);

        // Get the tuples list from the result and expected tables
        var resultTuples = (ArrayList<Comparable[]>) tuplesField.get(result);
        var expectedTuples = (ArrayList<Comparable[]>) tuplesField.get(expectedTable);
        
        // Verify the result is the same as expected
        assertEquals(expectedTuples.size(), resultTuples.size());
        for (int i = 0; i < expectedTuples.size(); i++) {
        	assertArrayEquals(expectedTuples.get(i), resultTuples.get(i));
        }
    }
    
}
