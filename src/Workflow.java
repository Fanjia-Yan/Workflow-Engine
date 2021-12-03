import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class Workflow implements Serializable{

    public String workflowId;
    public ArrayList<String> workflow_dictionary;
    public Graph dag;
    public boolean status;

    public Workflow(String workflowId,
                    ArrayList<String> graph_to_operator,
                    Graph dag){
        this.workflowId = workflowId;
        this.workflow_dictionary = graph_to_operator;
        this.status = false;
        this.dag =dag;
    }

}
