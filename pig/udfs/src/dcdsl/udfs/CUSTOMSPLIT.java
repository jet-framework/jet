package dcdsl.udfs;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class CUSTOMSPLIT extends EvalFunc<DataBag> implements Serializable
{
	
	BagFactory mBagFactory = BagFactory.getInstance();
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	//Pattern p = Pattern.compile("[^a-zA-Z0-9']+");
	
    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            if (input==null)
                return null;
            if (input.size()==0)
                return null;
            Object o = input.get(0);
            if (o==null)
                return null;
            DataBag output = mBagFactory.newDefaultBag();
            if (!(o instanceof String)) {
            	int errCode = 2114;
            	String msg = "Expected input to be chararray, but" +
                " got " + o.getClass().getName();
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            //String[] split = p.split((String)o);
            String[] split = ((String)o).split("[^a-zA-Z0-9']+");
            for (String x : split) {
            	output.add(mTupleFactory.newTuple(x));
            }
            return output;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public Schema outputSchema(Schema input) {
        
        try {
            Schema.FieldSchema tokenFs = new Schema.FieldSchema("token", 
                    DataType.CHARARRAY); 
            Schema tupleSchema = new Schema(tokenFs);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                    DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema(
                        "bag_of_tokenTuples",bagSchema, DataType.BAG);
            
            return new Schema(bagFs); 
            
            
            
        } catch (FrontendException e) {
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }   
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
//        s = new Schema();
//        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
//        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
//        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
    
}
