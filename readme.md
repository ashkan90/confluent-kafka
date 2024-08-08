
# Todo List

1. [ ] Batch Consumer
    - [ ] Batch Consumer with transient pool map memory to reduce memory allocation
    - [ ] Batch Consumer should be able to process messages without waiting all batch message to fill-up a map
2. [ ] Flexible yet Simple Producer
    - [ ] Producer must implement all the needs of the consumer such as batch
    - [ ] An interface to control state of produced message(s)
3. [x] Unbound internal and external Interface definitions such as ConsumerInstance
4. [ ] Integration Test
5. [x] Watermark query to get message count of given topic

# How to run locally to test package behaviour ?

1. Run `docker-compose up` 
2. Check `localhost:8080` to make sure kafka is running with no problem
3. Run `go run example_producer/` to first fill-up topic
4. Run `go run example/` to consume messages

#### After all of those stages, you'll need to see output such as below;
```
...
2024/08/03 01:32:23 Handle()  Producer example, message #1097564
2024/08/03 01:32:23 Handle()  Producer example, message #1097565
2024/08/03 01:32:23 Handle()  Producer example, message #1097561
2024/08/03 01:32:23 Handle()  Producer example, message #1097566
2024/08/03 01:32:23 Handle()  Producer example, message #1097568
2024/08/03 01:32:23 Handle()  Producer example, message #1097567
2024/08/03 01:32:23 Handle()  Producer example, message #1097569
2024/08/03 01:32:23 Handle()  Producer example, message #1097570
...
```


