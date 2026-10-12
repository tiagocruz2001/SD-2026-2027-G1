package tcp01;
import java.io.Serializable;

public class Person implements Serializable {
    private String name;
    private int year;
    private Place place;
    private static final long serialVersionUID = 1L;

    public Person (String name, Place place, int year){
        this.name = name;
        this.place = place;
        this.year = year;
    }
}
