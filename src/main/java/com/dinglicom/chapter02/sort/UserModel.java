package com.dinglicom.chapter02.sort;

/**
 * @author ly
 * @Date Create in 15:22 2021/2/9 0009
 * @Description
 */
public class UserModel {
    private String userId,name;
    private int age;
    public UserModel(String userId,String name,int age) {
        this.userId = userId;
        this.name = name;
        this.age = age;
    }
    @Override
    public String toString(){
        return "userId="+userId+",name="+name+",age="+age;
    }

    public String getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
