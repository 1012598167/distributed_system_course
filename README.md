# Window Join

## 待完成:

* 请在DSPPCode.storm.window_join中创建StormJoinBoltImpl, 继承StormJoinBolt, 实现抽象方法.
* 请在DSPPCode.storm.window_join中创建PrinterBoltImpl, 继承PrinterBolt, 实现抽象方法.

## 题目描述:

* 实现一个基于Window的Join操作,数据会在2s内生成，设计一个JoinBolt，实现流上的Join功能


    
  - Age 表
  
      | Id   | Age  | 
      | :--- | :--- | 
      | 0    | 20   |       
      | 1    | 21   |  
      | 2    | 22   | 

  - Gender表
  
      | Id   | Gender | 
      | :--- | :----- | 
      | 0    | male   |       
      | 1    | male   |  
      | 2    | female | 
  - Join之后的Age-Gender表
  
      | Id   | Gender | Age   |
      | :--- | :----- | :---- |
      | 0    | male   |  20   |
      | 1    | male   |  21   |
      | 2    | female |  22   |