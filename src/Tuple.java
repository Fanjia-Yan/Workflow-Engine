public class Tuple{
    /** Tuple class like Python Tuple*/
    public final String x;
    public final String y;
    public Tuple(String x, String y){
        this.x = x;
        this.y = y;
    }
    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
