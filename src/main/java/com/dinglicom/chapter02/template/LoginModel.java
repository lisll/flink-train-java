package com.dinglicom.chapter02.template;

/**
 * 封装进行登录控制所需要的数据
 */
public class LoginModel {
    /**
     * 登录人员的编号，通用的，可能是用户编号，也可能是工作人员编号
     */
    private String loginId;
    /**
     * 登录的密码
     */
    private String pwd;

    public void setLoginId(String loginId) {
        this.loginId = loginId;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getLoginId() {
        return loginId;
    }

    public String getPwd() {
        return pwd;
    }
}


/**
 *	登录控制的模板
 */
 abstract class LoginTemplate {
    /**
     * 判断登录数据是否正确，也就是是否能登录成功
     * @param lm 封装登录数据的Model
     * @return true表示登录成功，false表示登录失败
     */
    public final boolean login(LoginModel lm){
        //1：根据登录人员的编号去数据库获取相应的数据
        LoginModel dbLm = this.findLoginUser(lm.getLoginId());
        if(dbLm!=null){
            //2：对前台传过来的密码进行加密
            String encryptPwd = this.encryptPwd(lm.getPwd());
            //把加密后的密码设置回到登录数据模型里面
            lm.setPwd(encryptPwd);
            //3：判断是否匹配
            return this.match(lm, dbLm);
        }
        return false;
    }
    /**
     * 根据登录编号来查找和获取存储中相应的数据
     * @param loginId 登录编号
     * @return 登录编号在存储中相对应的数据
     */
    public abstract LoginModel findLoginUser(String loginId);
    /**
     * 对密码数据进行加密
     * @param pwd 密码数据
     * @return 加密后的密码数据
     */
    public String encryptPwd(String pwd){
        return pwd;
    }
    /**
     * 判断用户填写的登录数据和存储中对应的数据是否匹配得上
     * @param lm 用户填写的登录数据
     * @param dbLm 在存储中对应的数据
     * @return true表示匹配成功，false表示匹配失败
     */
    public boolean match(LoginModel lm,LoginModel dbLm){
        if(lm.getLoginId().equals(dbLm.getLoginId())
                && lm.getPwd().equals(dbLm.getPwd())){
            return true;
        }
        return false;
    }
}

/**
 * 普通用户登录控制的逻辑处理
 */
class NormalLogin extends LoginTemplate{
    public LoginModel findLoginUser(String loginId) {
        // 这里省略具体的处理，仅做示意，返回一个有默认数据的对象
        LoginModel lm = new LoginModel();
        lm.setLoginId(loginId);
        lm.setPwd("testpwd");
        return lm;
    }
}

/**
 * 工作人员登录控制的逻辑处理
 */
 class WorkerLogin extends LoginTemplate{
    public LoginModel findLoginUser(String loginId) {
        // 这里省略具体的处理，仅做示意，返回一个有默认数据的对象
        LoginModel lm = new LoginModel();
        lm.setLoginId(loginId);
        lm.setPwd("workerpwd");
        return lm;
    }
    public String encryptPwd(String pwd){
        //覆盖父类的方法，提供真正的加密实现
        //这里对密码进行加密，比如使用：MD5、3DES等等，省略了
        System.out.println("使用MD5进行密码加密");
        return pwd;
    }
 }

 class Client {
    public static void main(String[] args) {
//        //准备登录人的信息
//        LoginModel lm = new LoginModel();
//        lm.setLoginId("admin");
//        lm.setPwd("workerpwd");
//        //准备用来进行判断的对象
//        LoginTemplate lt = new WorkerLogin();
//        LoginTemplate lt2 = new NormalLogin();
//        //进行登录测试
//        boolean flag = lt.login(lm);
//        System.out.println("可以登录工作平台="+flag);
//
//        boolean flag2 = lt2.login(lm);
//        System.out.println("可以进行普通人员登录="+flag2);

        // 加强版
        //准备登录人的信息
        NormalLoginModel nlm = new NormalLoginModel();
        nlm.setLoginId("testUser");
        nlm.setPwd("testpwd");
        nlm.setQuestion("testQuestion");
        nlm.setAnswer("testAnswer");
        //准备用来进行判断的对象
        LoginTemplate lt3 = new NormalLogin2();
        //进行登录测试
        boolean flag3 = lt3.login(nlm);
        System.out.println("可以进行普通人员加强版登录="+flag3);

    }
}


/**
 * 封装进行登录控制所需要的数据，在公共数据的基础上，
 * 添加具体模块需要的数据
 */
class NormalLoginModel extends LoginModel{
    /**
     * 密码验证问题
     */
    private String question;
    /**
     * 密码验证答案
     */
    private String answer;

    public void setQuestion(String question) {
        this.question = question;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    public String getQuestion() {
        return question;
    }

    public String getAnswer() {
        return answer;
    }
}

/**
 * 普通用户登录控制加强版的逻辑处理
 */
 class NormalLogin2 extends LoginTemplate{
    public LoginModel findLoginUser(String loginId) {
        //注意一点：这里使用的是自己需要的数据模型了
        NormalLoginModel nlm = new NormalLoginModel();
        nlm.setLoginId(loginId);
        nlm.setPwd("testpwd");
        nlm.setQuestion("testQuestion");
        nlm.setAnswer("testAnswer");
        return nlm;
    }
    public boolean match(LoginModel lm,LoginModel dbLm){
        //这个方法需要覆盖，因为现在进行登录控制的时候，
        //需要检测4个值是否正确，而不仅仅是缺省的2个
        //先调用父类实现好的，检测编号和密码是否正确
        boolean f1 = super.match(lm, dbLm);
        if(f1){
            //如果编号和密码正确，继续检查问题和答案是否正确
            //先把数据转换成自己需要的数据
            NormalLoginModel nlm = (NormalLoginModel)lm;
            NormalLoginModel dbNlm = (NormalLoginModel)dbLm;
            //检查问题和答案是否正确
            if(dbNlm.getQuestion().equals(nlm.getQuestion())
                    && dbNlm.getAnswer().equals(nlm.getAnswer())){
                return true;
            }
        }
        return false;
    }
}

