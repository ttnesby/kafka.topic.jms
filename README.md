# kafka.topic.jms

The 'old' altinnkanal did two tasks
* receive web service from altinn
* transform to whatever and place message onto queue

Finally, different business systems read messages from different queues.

The new altinnkanal2 is a 'cleaner' version doing also two tasks
* receive web service from altinn
* wrap the generic batch part into a avro format and place the event in kafka topic

In utopia, the different business systems will get altinn-event from kafka and do their required mapping. Due to lack of utopia, this program is a mapper from  kafka topics to relevant MQs, transform included.

The supported set of altinn messages
* oppfolgingsplan service code 2913 and NavOppfPlan, with all known service edition codes 
* bankkontonr service code 2896, with all known service edition codes
* maalekort service code 4711, with all known service edition codes
* barnehageliste service code 4795, with all known service edition codes

In contrast to the 'old' altinnkanal, this program uses only one XSL file. See 'altinn2eifellesformat2018_03_16.xsl'



