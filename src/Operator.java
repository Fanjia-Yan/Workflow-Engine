import java.io.Serializable;
import java.util.ArrayList;

public class Operator <T extends Comparable<T>> implements Serializable {

    public String operatorId;
    public Callable code;

    public Operator(String operatorId,Callable code){
        this.code = code;
        this.operatorId = operatorId;
    }

    public Result execute(ArrayList<Result> input) throws Exception {
        T output_payload = (T) code.call(input);
        return new Result(operatorId,output_payload);
    }
}
