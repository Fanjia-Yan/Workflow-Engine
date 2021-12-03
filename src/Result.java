
public class Result <T extends Comparable<T>>{

    public String operatorId;
    public T payload;

    public Result(String operatorId, T payload){
        this.operatorId = operatorId;
        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }

    public String getOperatorId() {
        return operatorId;
    }
}
