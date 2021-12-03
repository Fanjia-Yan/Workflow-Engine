import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class EngineTest {
    @Before
    public void setup() {
        Engine.init(4096 * 4);
    }

    @Test
    public void serializable_test() {
        /** test the class in the implementation can be serialized */
        Graph graph = new Graph(5);
        graph.addEdge(1, 2);
        Util.serialize(graph);
        String workflow_id = "1223";
        Util.serialize(workflow_id);
        ArrayList<String> arr = new ArrayList<>();
        Util.serialize(arr);
        Iterator<Integer> iter = graph.iterator();
        Workflow wf = new Workflow(workflow_id, arr, graph);
        Util.serialize(wf);
    }

    @Test
    public void simple_register_test() throws Exception {
        /** simple workflow oper001 + oper022 -> oper003 */
        String workflow_id = "wf001";
        HashMap<String, Callable> operators = new HashMap<>();
        Callable<Integer> oper001 = (x) -> 1;
        Callable<Integer> oper002 = (x) -> 2;
        Callable<Integer> oper003 = (x) -> (int) (x.get(0)).getPayload() + (int) (x.get(1)).getPayload();
        operators.put("oper001", oper001);
        operators.put("oper002", oper002);
        operators.put("oper003", oper003);
        ArrayList<Tuple> dependencies = new ArrayList<>();
        dependencies.add(new Tuple("oper001", "oper003"));
        dependencies.add(new Tuple("oper002", "oper003"));

        /** execute register_workflow*/
        Engine.register_workflow(workflow_id, operators, dependencies);

        /** check if workflow successfully stored in disk and the recovered value matches */
        File workflow_file = Util.join(Engine.WORKFLOW_DIR, workflow_id);
        assertTrue(workflow_file.exists());

        if (!workflow_file.exists()) {
            return;
        }

        Workflow workflow = Util.readObject(workflow_file, Workflow.class);
        assertEquals(workflow.workflowId, workflow_id);

        /** check if the topological sort gives the right order */
        Iterator<Integer> iter = workflow.dag.iterator();
        int node = iter.next();
        ArrayList<String> dict = workflow.workflow_dictionary;
        assertTrue(dict.get(node).equals("oper001") || dict.get(node).equals("oper002"));
        node = iter.next();
        assertTrue(dict.get(node).equals("oper001") || dict.get(node).equals("oper002"));
        node = iter.next();
        assertEquals(dict.get(node), "oper003");
        assertFalse(iter.hasNext());

        /** check if all the operators are stored succesfully */
        for (String oper_id : operators.keySet()) {
            File operator_file = Util.join(Engine.OPERATOR_DIR, oper_id);
            assertTrue(operator_file.exists());
            if (!operator_file.exists()) {
                return;
            }
            Operator operate = Util.readObject(operator_file, Operator.class);

            /** check if all operator's attribute retains the same function */
            ArrayList<Result> result = new ArrayList<>();
            assertEquals(operate.operatorId, oper_id);
            if (oper_id == "oper003") {
                Result result1 = new Result("oper001", 1);
                Result result2 = new Result("oper002", 2);
                result.add(result1);
                result.add(result2);
            }
            assertEquals(operate.operatorId, oper_id);
            assertEquals(operate.code.call(result), operators.get(oper_id).call(result));
        }
    }

    @Test
    public void simple_execute_test() throws Exception {
        /*Engine.cache.flush(); */
        String workflow_id = "wf001";
        ArrayList<Result> execution_result = Engine.execute_workflow(workflow_id);
        /**check the result of execution */
        assertEquals(execution_result.get(0).payload, 3);
    }

    @Test
    public void complicated_execute_test() throws Exception {
        /** workflow 002 :
         * dependency : 1(value of 1) + 2(value of 2) -> 3(value of 3), 6 + 3 -> 5(value of 9), 3 + 2 ->4(value of 5),
         * 4 + 1->6(value of 6), 2 + 4 + 6-> 7(value of 13)
         *                      1       2
         *                          3
         *                              4
         *                          6
         *                          "5"  "7"
         * the executed result should contain "oper005" and "oper007" i.e. 9 and 13
         * */
        String workflow_id = "wf002";
        HashMap<String, Callable> operators = new HashMap<>();
        Callable<Integer> oper001 = (x) -> 1;
        Callable<Integer> oper002 = (x) -> 2;
        Callable<Integer> oper003 = (x) -> (int) (x.get(0)).getPayload() + (int) (x.get(1)).getPayload();
        Callable<Integer> oper004 = (x) -> (int) (x.get(0)).getPayload() + (int) (x.get(1)).getPayload();
        Callable<Integer> oper005 = (x) -> (int) (x.get(0)).getPayload() + (int) (x.get(1)).getPayload();
        Callable<Integer> oper006 = (x) -> (int) (x.get(0)).getPayload() + (int) (x.get(1)).getPayload();
        Callable<Integer> oper007 = (x) -> (int) (x.get(0)).getPayload() + (int) (x.get(1)).getPayload() + (int) (x.get(2)).getPayload();
        operators.put("oper011", oper001);
        operators.put("oper012", oper002);
        operators.put("oper013", oper003);
        operators.put("oper014", oper004);
        operators.put("oper015", oper005);
        operators.put("oper016", oper006);
        operators.put("oper017", oper007);
        ArrayList<Tuple> dependencies = new ArrayList<>();
        dependencies.add(new Tuple("oper011", "oper013"));
        dependencies.add(new Tuple("oper012", "oper013"));
        dependencies.add(new Tuple("oper016", "oper015"));
        dependencies.add(new Tuple("oper013", "oper015"));
        dependencies.add(new Tuple("oper013", "oper014"));
        dependencies.add(new Tuple("oper012", "oper014"));
        dependencies.add(new Tuple("oper014", "oper016"));
        dependencies.add(new Tuple("oper011", "oper016"));
        dependencies.add(new Tuple("oper012", "oper017"));
        dependencies.add(new Tuple("oper014", "oper017"));
        dependencies.add(new Tuple("oper016", "oper017"));
        Engine.register_workflow(workflow_id, operators, dependencies);
        ArrayList<Result> results = Engine.execute_workflow("wf002");
        System.out.println((int) results.get(0).payload);
        System.out.println((int) results.get(1).payload);
        assertTrue((int) results.get(0).payload == 9 || (int) results.get(0).payload == 13);
        assertTrue((int) results.get(1).payload == 13 || (int) results.get(1).payload == 9);
    }

    @Test
    public void one_operator_workflow_test() throws Exception {
        /** test workflow with one operator */
        String workflow_id = "wf003";
        HashMap<String, Callable> operators = new HashMap<>();
        Callable<Integer> oper001 = (x) -> 1;
        operators.put("oper021", oper001);
        ArrayList<Tuple> dependencies = new ArrayList<>();
        Engine.register_workflow(workflow_id, operators, dependencies);
        ArrayList<Result> results = Engine.execute_workflow("wf003");
        assertTrue((int) results.get(0).payload == 1);
    }

    @Test
    public void no_operator_workflow_test()throws Exception{
        /** test workflow with no operator */
        String workflow_id = "wf004";
        HashMap<String, Callable> operators = new HashMap<>();
        ArrayList<Tuple> dependencies = new ArrayList<>();
        Engine.register_workflow(workflow_id, operators, dependencies);
        ArrayList<Result> results = Engine.execute_workflow("wf004");
        assertTrue(results.size() == 0);
    }
}
