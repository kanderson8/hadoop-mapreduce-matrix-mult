import java.net.URI;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Matrix 
{
	
	public static void main(String[] args) throws Exception 
	{
		
		if(args.length !=5)
		{
			System.err.println("Usage : Matrix <input path> <output path> m n p");
			System.exit(-1);
		}
                System.err.println("Input path: " + args[0]);
                System.err.println("Output path: " + args[1]);
		
		Configuration conf = new Configuration();
		conf.set("dimM", args[2]); // set matrix dimension m
                conf.set("dimN", args[3]); //set matrix dimension n
                conf.set("dimP", args[4]); //set matrix dimension p
		Job job = Job.getInstance(conf);
		
		FileSystem fs = FileSystem.get(conf);
		
		job.setJarByClass(Matrix.class);
		
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
				
		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(MatrixReducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
				
		if(!fs.exists(input)) 
		{
			System.err.println("Please provide a valid HFDS input path");
			System.exit(-1);
		}
		if(fs.exists(output)) 
		{
			fs.delete(output, true);
		}
		FileInputFormat.addInputPath(job, input);
		
		FileOutputFormat.setOutputPath(job, output);
		long start = new Date().getTime();		
		job.waitForCompletion(true);
                long end = new Date().getTime();
                System.out.println("Job took: " + (end - start) + " milliseconds");
                fs.close();
				
	}

        public static class MatrixMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	/**
	 * Produces a key-value output where the key is the matrix location (ex: 01) and the value is of the form
         * matrix,index,value (ex: a,1,5)
         * NOTE: The index is the column for entries of the matrix A and the row for the entries of matrix B 
         */
	
	@Override
	public void map(LongWritable key, Text value,Context context)
						throws IOException, InterruptedException
	{
                //entries are matrix, row, column, value
                String[] entry = value.toString().split(",");

		String matrixName = entry[0];
                String matrixRow = entry[1];
                String matrixCol = entry[2];
                String matrixVal = entry[3];
                int m = Integer.parseInt( context.getConfiguration().get("dimM") );
                int p = Integer.parseInt( context.getConfiguration().get("dimP") );
		
		
		if (matrixName.matches("a"))
		{
			for (int i =0; i < m; i++) 
			{
                                String rowCol = matrixRow + i;
                                //System.out.println(String.format("input: %s, key: %s, val: %s", value.toString(), entry[1].trim() + i,
                                              //String.format("a,%s,%s", entry[2].trim(), entry[3].trim())));
				context.write(new Text(rowCol),
                                              new Text(String.format("a,%s,%s", matrixCol, matrixVal)));
			}
		}
		
		if (matrixName.matches("b"))
		{
			for (int i =0; i < p; i++)
			{
                                String rowCol = i + matrixCol;
                                //System.out.println(String.format("input: %s, key: %s, val: %s", value.toString(), i + entry[2].trim(),
                                  //            String.format("a,%s,%s", entry[1].trim(), entry[3].trim())));
				context.write(new Text(rowCol),
                                              new Text(String.format("b,%s,%s", matrixRow, matrixVal)));
			}
		}
		
	}
	

    }


    public static class MatrixReducer extends Reducer<Text, Text, Text, DoubleWritable>
    {

	/**
         * Produces a key value output where the key is the matrix location (ex: 01) and the value is the result of the matrix
         * multiplication (ex: 56)
	 */
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
						throws IOException, InterruptedException 
	{
		
                int n = Integer.parseInt( context.getConfiguration().get("dimN") );
		
		double[] row = new double[n];
		double[] col = new double[n];
		
                for (Text val : values)
		{
			String[] entries = val.toString().split(",");
                        String matrixName = entries[0];
                        String multiplicationIndex = entries[1];
                        String multiplicationValue = entries[2];

                        int index = Integer.parseInt(multiplicationIndex);

                        if (matrixName.matches("a"))
			{
				row[index] = Double.parseDouble(multiplicationValue);
			}
			if (matrixName.matches("b"))
			{
				col[index] = Double.parseDouble(multiplicationValue);
			}
		}
		
		double sum = 0;
		for (int i = 0 ; i < n; i++)
		{
			sum += row[i]*col[i];
		}
		context.write(key, new DoubleWritable(sum));
	
	}
	
    }


}
