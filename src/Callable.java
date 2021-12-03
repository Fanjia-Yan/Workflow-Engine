import java.io.Serializable;
import java.util.ArrayList;

public interface Callable<T> extends Serializable {
    /**
     * Callable type, mimic python's version
     */
    T call(ArrayList<Result> input) throws Exception;
}