import javax.print.attribute.ResolutionSyntax;
import java.io.File;
import java.util.*;

/**
 * Potential improvements:
 * 1. hashing the workflow serializable to hashcode to reduce the memory cost
 * 2. it will be important for us to gauge the size of the workflow/operator to get better cache performance
 *
 */
public class Engine {

    /** directory that might be used */
    public static final File CWD = new File(System.getProperty("user.dir"));
    public static final File WORKFLOW_DIR = Util.join(CWD,"workflow");
    public static final File OPERATOR_DIR = Util.join(CWD,"operator");
    /** Cache for workflow and operator */
    public static final boolean cache_status = true;
    public static Cache cache;

    /** initialize the file persistence directory in current working directory
     * and initialize cache if necessary**/
    public static void init(int capacity){
        if(!WORKFLOW_DIR.exists()){
            WORKFLOW_DIR.mkdirs();
        }
        if(!OPERATOR_DIR.exists()){
            OPERATOR_DIR.mkdirs();
        }
        if(cache_status == true){
            cache = new Cache(capacity);
        }
    }

    public static void register_workflow(String workflowId,
                                  HashMap<String, Callable> operators,
                                  ArrayList<Tuple> dependencies){
        /**
         * Register a workflow given the workflow id, its operators, and dependencies.
         * 	Args:
         * 	    workflowId: the ID of the workflow.
         * 			operators: a map whose keys are the IDs of the operators of this workflow,
         * 								 and whose values are the code of the operators.
         * 			dependencies: a map that contains the operators' dependencies. For example,
         * 										if dependencies[a] = b, then operator (with ID) b depends on a.
         * 	Returns:
         * 	    this API does not return anything.
         */

        /** put the dependencies into a DAG Graph with Graph having
         * topological sort. See implementation in Graph.Java
         */
        int num_vertices = operators.size();

        ArrayList<String> graph_to_operator = new ArrayList<>();
        for(String operator_id : operators.keySet()) {
            graph_to_operator.add(operator_id);
        }

        Graph dag = new Graph(num_vertices);
        for(Tuple key : dependencies) {
            if(!graph_to_operator.contains(key.x) || !graph_to_operator.contains(key.y)) {
                System.out.println("operator in the dependencies does not exist");
                return;
            }
            dag.addEdge(graph_to_operator.indexOf(key.x), graph_to_operator.indexOf(key.y));
        }

        /** store it as workflow */
        Workflow wf = new Workflow(workflowId,graph_to_operator,dag);

        /** save the workflow into workflow folder */
        File workflow_file = Util.join(WORKFLOW_DIR,workflowId);
        if(workflow_file.exists()){
            workflow_file.delete();
        }
        Util.writeObject(workflow_file,wf);

        /** save the operators into operator folder */
        for(String operator_id : operators.keySet()){
            Operator opt = new Operator(operator_id,operators.get(operator_id));
            File operator_file = Util.join(OPERATOR_DIR,operator_id);
            if(operator_file.exists()){
                operator_file.delete();
            }
            Util.writeObject(operator_file,opt);
        }
    }

    public static ArrayList<Result> execute_workflow(String workflowid) throws Exception {
        /**
         * Execute a workflow given the workflow id.
         *  Args:
         *      workflowId: the ID of the workflow to be executed.
         * Returns:
         *      a list of `result` of operators who are not dependencies of
         *          any other operators (also called the "sink" operators).
         */

        byte[] cache_workflow = cache.find(workflowid);
        Workflow wf;
        if(cache_workflow != null && cache_status){
            /** cache hit and load directly from cache, update timestamp */
            wf = Util.deserialize(cache_workflow,Workflow.class);
            cache.update(workflowid);
        }else {
            /** cache miss and load the workflow with workflowid, add it in cache*/
            File workflow_file = Util.join(WORKFLOW_DIR, workflowid);
            if (!workflow_file.exists()) {
                System.out.println("workflow does not exist");
                return null;
            }
            wf = Util.readObject(workflow_file, Workflow.class);
            byte[] wf_serialized = Util.serialize(wf);
            cache.add(workflowid,wf_serialized);
        }

        /** return array of result */
        ArrayList<Result> ret= new ArrayList<>();
        /** Dictionary: dictionary.get(index) = workflowid */
        ArrayList<String> dictionary = wf.workflow_dictionary;
        /** the graph stored in the workflow generated from register */
        Graph dag = wf.dag;
        /** topological iterator of the operators order from the workflow */
        Iterator<Integer> workflow_iterator = dag.iterator();
        /** Dictionary {Operator : Result} */
        HashMap<Integer, Result> executed_result = new HashMap<>();

        /** begin execution **/
        while(workflow_iterator.hasNext()) {
            /** read the operator **/
            int node = workflow_iterator.next();
            String operator_id = dictionary.get(node);
            byte[] cache_operator = cache.find(operator_id);
            Operator operator;
            if(cache_operator != null && cache_status){
                /** if cache hit directly use cache and update its timestamp*/
                operator = Util.deserialize(cache_operator,Operator.class);
                cache.update(operator_id);
            }else {
                /** if cache miss, load the operator from disk and save it in cache */
                File operator_file = Util.join(OPERATOR_DIR, operator_id);
                if (!operator_file.exists()) {
                    System.out.println("operator does not exist");
                    return null;
                }
                operator = Util.readObject(operator_file, Operator.class);
                byte[] operator_serialized = Util.serialize(operator);
                cache.add(operator_id,operator_serialized);
            }
            /** find its dependencies by using Graph */
            ArrayList<Integer> dependent = dag.dependencies(node);
            ArrayList<Result> dependent_result = new ArrayList<>();
            for (int dependent_index : dependent) {
                dependent_result.add(executed_result.get(dependent_index));
            }
            /** get the Result */
            Result result = operator.execute(dependent_result);
            /** if out-degree is 0 then store it as return value,
             * else it is a by-process so store it in the dictionary.
             */
            if (dag.outDegree(node) == 0) {
                ret.add(result);
            } else {
                executed_result.put(node, result);
            }
        }
        return ret;
    }
}
