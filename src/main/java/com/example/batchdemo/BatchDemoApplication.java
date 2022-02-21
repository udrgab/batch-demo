package com.example.batchdemo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;

@EnableBatchProcessing
@SpringBootApplication
public class BatchDemoApplication {

    public static class Person {
        private int age;
        private String firstName;
        private String email;

        public Person() {
        }

        public Person(int age, String firstName, String email) {
            this.age = age;
            this.firstName = firstName;
            this.email = email;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }

    @Bean
    FlatFileItemReader<Person> fileReader(@Value("${input}") Resource in) throws Exception {
        return new FlatFileItemReaderBuilder<Person>()
                .resource(in)
                .name("file-reader")
                .targetType(Person.class)
                .delimited().delimiter(",").names(new String[]{"firstName", "age", "email"})
                .build();
    }

    @Bean
    JdbcBatchItemWriter<Person> jdbcWriter(DataSource ds){
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(ds)
                .sql("insert into PEOPLE(AGE, FIRST_NAME, EMAIL) values(:age,:firstName,:email)")
                .beanMapped()
                .build();
    }

    @Bean
    Job job(JobBuilderFactory jbf,
            StepBuilderFactory sbf,
            ItemReader<? extends Person> ir,
            ItemWriter<? super Person> iw) {
        Step s1 = sbf.get("file-db")
                .<Person, Person>chunk(100)
                .reader(ir)
                .writer(iw)
                .build();

        return jbf.get("etl")
                .incrementer(new RunIdIncrementer())
                .start(s1)
                .build();

    }

    public static void main(String[] args) {
        System.setProperty("input", "file://" + new File("C:/Users/gudrea/Desktop/in.csv").getAbsolutePath());
        System.setProperty("output", "file://" + new File("C:/Users/gudrea/Desktop/out.csv").getAbsolutePath());
        SpringApplication.run(BatchDemoApplication.class, args);
    }

}
