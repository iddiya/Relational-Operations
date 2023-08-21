/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.*;
import static java.lang.System.arraycopy;
import static java.lang.System.out;

/****************************************************************************************
 * The Table class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key (the attributes forming). 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.LINHASH_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        return switch (mType) {
        case TREE_MAP    -> new TreeMap <> ();
        case LINHASH_MAP -> new LinHashMap <>(KeyType.class, Comparable[].class); 
        //case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
        default          -> null;
        }; // switch
    } // makeMap

    /************************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
     *
     * @param arr1  the first array
     * @param arr2  the second array
     * @return  a wider array containing all the values from arr1 and arr2
     */
    public static <T> T [] concat (T [] arr1, T [] arr2)
    {
        T [] result = Arrays.copyOf (arr1, arr1.length + arr2.length);
        arraycopy (arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();
    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        var attrs     = attributes.split (" ");
        var colDomain = extractDom (match (attrs), domain);
        var newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();
        //  T O   B E   I M P L E M E N T E D 
        for (Comparable[] tuple : tuples) {
            Comparable[] newRow = new Comparable[attrs.length];
            int index = 0;
            for (String attr : attrs) {
                int attrIndex = Arrays.asList(attribute).indexOf(attr);
                if (attrIndex != -1) {
                    newRow[index] = tuple[attrIndex];
                    index++;
                }
            }
            rows.add(newRow);
        }
        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given simple condition on attributes/constants
     * compared using an <op> ==, !=, <, <=, >, >=.
     *
     * #usage movie.select ("year == 1977")
     *
     * @param condition  the check condition as a string for tuples
     * @return  a table with tuples satisfying the condition
     */
    public Table select (String condition)
    {
        out.println ("RA> " + name + ".select (" + condition + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D
        // Parsing
        String[] tokens = condition.split("\\s+"); // Split by space
        String attr = tokens[0]; // Attribute
        String op = tokens[1]; // Operator
        String value = tokens[2]; // Value

        // Determine the attribute index
        int attrIndex = Arrays.asList(attribute).indexOf(attr);

        // For loop to iterate over tuples and apply the condition
        // Iterate over tuples and apply the condition
        for (Comparable[] tuple : tuples) {
            Comparable attrValue = tuple[attrIndex];

            // Compare the attribute value with the given value based on the operator
            boolean satisfiesCondition = false;
            switch (op) {
                case "==":
                    satisfiesCondition = attrValue.equals(Integer.parseInt(value));
                    break;
                case "!=":
                    satisfiesCondition = !attrValue.equals(Integer.parseInt(value));
                    break;
                case "<":
                    satisfiesCondition = attrValue.compareTo(Integer.parseInt(value)) < 0;
                    break;
                case "<=":
                    satisfiesCondition = attrValue.compareTo(Integer.parseInt(value)) <= 0;
                    break;
                case ">":
                    satisfiesCondition = attrValue.compareTo(Integer.parseInt(value)) > 0;
                    break;
                case ">=":
                    satisfiesCondition = attrValue.compareTo(Integer.parseInt(value)) >= 0;
                    break;
                default:
                    out.println("Invalid operator: " + op);
                    break;
            }
            // If the tuple satisfies the condition, add it to the result
            if (satisfiesCondition) {
                rows.add(tuple);
            }
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.  INDEXED SELECT ALGORITHM.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D  - Project 2
		
		// Get the tuples with correct keyVal
		rows.add(index.get(keyVal));

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D o
        // Add tuples from the current table
        for (Comparable[] tuple : tuples) {
            rows.add(tuple);
        }

        // Add tuples from table2 if they are not already present
        for (Comparable[] tuple : table2.tuples) {
            if (!rows.contains(tuple)) {
                rows.add(tuple);
            }
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> (); // store the resulting rows after the set difference operation

        //  T O   B E   I M P L E M E N T E D
        for (Comparable[] row1 : this.tuples) {//iterates each row in the current this.tuples table
            boolean foundMatch = false;
            for (Comparable[] otherRow : table2.tuples) {//iterates each row in the table2.tuples table
                if (Arrays.equals(row1, otherRow)) {//checks if this.row in the current table is equal to the row in table2
                    foundMatch = true;
                    break;
                }
            }
            if (!foundMatch) {
                rows.add(row1);//if no match founded; row is added to the rows list
            }
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by appending "2" to the end of any duplicate attribute name.  Implement using
     * a NESTED LOOP JOIN ALGORITHM.
     *
     * #usage movie.join ("studioName", "name", studio)
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");


        var t_attrs = attributes1.split (" ");
        var u_attrs = attributes2.split (" ");
        int l1 = t_attrs.length;
        int l2 = u_attrs.length;
        //  T O   B E   I M P L E M E N T E D
        ArrayList<String> dups = new ArrayList<String>(); //STORES DUPLICATE ATTRIBUTES ACCORDING TO NAMES
        for(int i=0;i<l1 ; i++)
        {
            for(int j=0;j<l2;j++)
            {
                if(t_attrs[i].equals(u_attrs[j]))
                {   dups.add(t_attrs[i]);
                }
            }
        }
        List <Comparable []> rows1 = new ArrayList <> ();
        int flag = 1;
        for (Comparable[] tup1 : tuples) {
            for (Comparable[] tup2 : table2.tuples)
            {   flag = 1;
               for(int i=0;i<l1;i++)
               {
                   int t1a = Arrays.asList(attribute).indexOf(t_attrs[i]);
                   int t2a = Arrays.asList(table2.attribute).indexOf(u_attrs[i]);
                   if(tup1[t1a]!=tup2[t2a])
                   {
                       flag = 0;
                   }
               }
               if(flag == 1)
               {
                   rows1.add(concat(tup1,tup2));
               }
            }
        }
      String[] s = new String[100];
        s = table2.attribute;
        String[] s1 = attribute;
        int l3 = s1.length;
        int l = s.length;
        for(int i=0;i<l3;i++) {
            for(int j=0;j<l;j++)
                if (s1[i].equals(s[j])) {
                    s[i] = s[i] + "2";
            }
        }
        return new Table (name + count++, concat (attribute,s ),
                                          concat (domain, table2.domain), this.key, rows1);
    }
       // join

    /************************************************************************************
     * Join this table and table2 by performing a "theta-join".  Tuples from both tables
     * are compared attribute1 <op> attribute2.  Disambiguate attribute names by appending "2"
     * to the end of any duplicate attribute name.  Implement using a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioName == name", studio)
     *
     * @param condition  the theta join condition
     * @param table2     the rhs table in the join operation
     * @return  a table with tuples satisfying the condition
     */
     public Table join (String condition, Table table2)
    {
        out.println ("RA> " + name + ".join (" + condition + ", " + table2.name + ")");
        var rows = new ArrayList <Comparable []> ();
        //  T O   B E   I M P L E M E N T E D
        var atr = condition.split (" ");
        int t1a = Arrays.asList(this.attribute).indexOf(atr[0]);
        int t2a = Arrays.asList(table2.attribute).indexOf(atr[2]);
        for (Comparable[] tup1 : this.tuples) {
            for (Comparable[] tup2 : table2.tuples)
            {
                switch (atr[1]) {
                    case "==":
                        if(tup1[t1a] == tup2[t2a])
                        rows.add(concat(tup1,tup2));
                        break;
                    case "!=":
                        if(tup1[t1a] != tup2[t2a])
                            rows.add(concat(tup1,tup2));
                        break;
                    case "<":
                        if(tup1[t1a].compareTo(tup2[t2a]) <0)
                            rows.add(concat(tup1,tup2));
                        break;
                    case "<=":
                        if(tup1[t1a].compareTo(tup2[t2a]) <=0)
                            rows.add(concat(tup1,tup2));
                        break;
                    case ">":
                        if(tup1[t1a].compareTo(tup2[t2a]) >0)
                            rows.add(concat(tup1,tup2));
                        break;
                    case ">=":
                        if(tup1[t1a].compareTo(tup2[t2a]) >=0)
                            rows.add(concat(tup1,tup2));
                        break;
                    default:
                        out.println("Invalid operator: " + atr[1]);
                        break;
                }
            }
        }
        String[] s = new String[100];
        String[] s2 = attribute;
        s = table2.attribute;
        int l1 = s2.length;
        int l3 = s.length;
        for(int i=0;i<l1;i++) {
            for(int j=0;j<l3;j++)
            if (s2[i].equals(s[j])) {
                s[i] = s[i] + "2";
            }
        }
        return new Table (name + count++, concat (attribute, s),
                                          concat (domain, table2.domain), key, rows);
    }
       // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above equi-join,
     * but implemented using an INDEXED JOIN ALGORITHM.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
     public Table i_join (String attributes1, String attributes2, Table table2)
     {
         out.println ("RA> " + name + ".i_join (" + attributes1 + ", " + attributes2 + ", "
                                                + table2.name + ")");

         var t_attrs = attributes1.split (" ");
         var u_attrs = attributes2.split (" ");
         // index on the join column of table2
         for (Comparable[] tup : table2.tuples) 
        	 index.put (new KeyType (tup[table2.col(u_attrs[0])]), tup);

         // Join the tables using the index
         List <Comparable []> rows1 = new ArrayList <> ();
         for (Comparable[] tup1 : tuples) {
             Comparable[] tup2 = index.get(new KeyType(tup1[col(t_attrs[0])]));
             if (tup2 != null) rows1.add(concat(tup1,tup2));
         }

         return new Table (name + count++, concat (attribute, table2.attribute),
                                           concat (domain, table2.domain), key, rows1);
     } // i_join


    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {

        //  D O   N O T   I M P L E M E N T

        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");
        var rows = new ArrayList <Comparable []> ();
        //  T O   B E   I M P L E M E N T E D
        // FIX - eliminate duplicate columns
        String[] t_attrs = new String[100];
        String[] u_attrs = new String[100];
        t_attrs = attribute;
        u_attrs = table2.attribute;
        int l1 = t_attrs.length;
        int l2 = u_attrs.length;
        String[] dups = new String[l1];
        int y = 0;
        String[] finalU_attrs = u_attrs;
        for(int i=0;i<l1 ; i++)
        {
            for(int j=0;j<l2;j++)
            {

                int finalJ = j;
                if(t_attrs[i].equals(u_attrs[j]) && !(Arrays.stream(dups).anyMatch(g -> g == finalU_attrs[finalJ])))
                {   dups[y++] = u_attrs[j];
                }
            }
        }
        int flag = 1;
        for (Comparable[] tup1 : tuples) {
            for (Comparable[] tup2 : table2.tuples)
            {   flag = 1;
                int[] ind = new int[tup2.length];
                int k=0;
                for(int i=0;i<l1;i++)
                {
                    int t1a = Arrays.asList(attribute).indexOf(dups[i]);
                    int t2a = Arrays.asList(table2.attribute).indexOf(dups[i]);
                    ind[k++] = t2a;
                    //out.print(ind[0]);
                    if(t1a!= -1 && t2a!=-1 && tup1[t1a]!=tup2[t2a])
                    {
                        flag = 0;
                    }
                }
                if(flag == 1)
                {   int t=0;
                    Comparable[] s2 = new  Comparable[tup2.length -y];
                    for(int j=0;j<tup2.length;j++)
                    {
                        if(!Arrays.toString(ind).contains(String.valueOf(j))) {
                            out.println(Arrays.toString(ind) +"   "+j);
                            s2[t++] = (tup2[j]);
                        }
                    }
                    if(s2.length==0 ||s2[0]==null)
                        rows.add(tup1);
                    else
                    rows.add(concat(tup1,s2));
                }
            }
        }

        String[] s = new String[100];
        s = table2.attribute;
        int l4 = s.length;
        String[] p = new String[l4-y];
        String[] t = new String[attribute.length+l4-y];
        for(int i=0,k=0;i<l4;i++) {
            if (!Arrays.toString(dups).contains(s[i])){
                p[k] = s[i];
                k++;
            }
        }
        if(p.length==0 || p[0] == null)
            t = attribute;
        else
            t = concat(attribute,p);

        return new Table (name + count++, t,
                                          concat (domain, table2.domain), key, rows);


    }
       // join

    /************************************************************************************
     * Return the column position for the given attribute name or -1 if not found.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (var i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;       // -1 => not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("Star_Wars", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            var keyVal = new Comparable [key.length];
            var cols   = match (key);
            for (var j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        out.print ("| ");
        for (var a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        for (var tup : tuples) {
            out.print ("| ");
            for (var attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (var j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        var tup    = new Comparable [column.length];
        var colPos = match (column);
        for (var j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in array) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a array of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;      // change once implemented
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        var classArray = new Class [className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos  the column positions to extract.
     * @param group   where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        var obj = new Class [colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom
} // Table class
