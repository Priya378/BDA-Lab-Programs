/* To count how many words belong to each of the 4 categories:
 tiny: 1
 small: 2-4
 medium: 5-9
 big: more than 10
*/
//In the Mapper:
while(tokenizer.hasMoreTokens()){
    String word = tokenizer.nextToken();
    int length = word.length();
    String c = ((length == 1)? "tiny":
    (length >= 2 && length <= 4) ? "small":
    (length >= 5 && length <= 9) ? "medium":
    "big");
    category.set(c) // category from class Text was declared
    context.write(category, one);
    //Context context
    //IntWritable one = new IntWritable(1)
}

//In the Reducer:
int sum = 0;
for (IntWritable val: values){
    sum += val.get();
}
result.set(sum);
context.write(key, result);