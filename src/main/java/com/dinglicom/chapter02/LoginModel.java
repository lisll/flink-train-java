package com.dinglicom.chapter02;


/**
 * 看了这里的实现示例，是不是很简单。但是，仔细看看，总会觉得有点问题，两种登录的实现太相似了，
 * 现在是完全分开，当作两个独立的模块来实现的，如果今后要扩展功能，
 * 比如要添加“控制同一个编号同时只能登录一次”的功能，那么两个模块都需要修改，是很麻烦的。
 * 而且，现在的实现中，也有很多相似的地方，显得很重复。另外，具体的实现和判断的步骤混合在一起，
 * 不利于今后变换功能，比如要变换加密算法等。总之，上面的实现，有两个很明显的问题：
 * 一是重复或相似代码太多；二是扩展起来很不方便。那么该怎么解决呢？
 * 该如何实现才能让系统既灵活又能简洁的实现需求功能呢？
 */



/**
 * 描述登录人员登录时填写的信息的数据模型
 */
public class LoginModel {
    private String userId,pwd;

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getUserId() {
        return userId;
    }

    public String getPwd() {
        return pwd;
    }
}

/**
 * 描述普通用户信息的数据模型
 */
 class UserModel {
    private String uuid,userId,pwd,name;

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUuid() {
        return uuid;
    }

    public String getUserId() {
        return userId;
    }

    public String getPwd() {
        return pwd;
    }

    public String getName() {
        return name;
    }
}


 class LoginModelWorker{
    private String workerId,pwd;

     public void setWorkerId(String workerId) {
         this.workerId = workerId;
     }

     public void setPwd(String pwd) {
         this.pwd = pwd;
     }

     public String getWorkerId() {
         return workerId;
     }

     public String getPwd() {
         return pwd;
     }
 }

/**
 * 描述工作人员信息的数据模型
 */
 class WorkerModel {
    private String uuid,workerId,pwd,name;

    public String getUuid() {
        return uuid;
    }

    public String getWorkerId() {
        return workerId;
    }

    public String getPwd() {
        return pwd;
    }

    public String getName() {
        return name;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public void setName(String name) {
        this.name = name;
    }
}


/**
 * 普通用户登录控制的逻辑处理
 */
 class NormalLogin {
    /**
     * 判断登录数据是否正确，也就是是否能登录成功
     * @param lm 封装登录数据的Model
     * @return true表示登录成功，false表示登录失败
     */
    public boolean login(LoginModel lm) {
        //1：从数据库获取登录人员的信息， 就是根据用户编号去获取人员的数据
        UserModel um = this.findUserByUserId(lm.getUserId());
        //2：判断从前台传递过来的登录数据，和数据库中已有的数据是否匹配
        //先判断用户是否存在，如果um为null，说明用户肯定不存在
        if (um != null) {
            //如果用户存在，检查用户编号和密码是否匹配
            if (um.getUserId().equals(lm.getUserId())
                    && um.getPwd().equals(lm.getPwd())) {
                return true;
            }
        }
        return false;
    }
    /**
     * 根据用户编号获取用户的详细信息
     * @param userId 用户编号
     * @return 对应的用户的详细信息
     */
    private UserModel findUserByUserId(String userId) {
        // 这里省略具体的处理，仅做示意，返回一个有默认数据的对象
        UserModel um = new UserModel();
        um.setUserId(userId);
        um.setName("test");
        um.setPwd("test");
        um.setUuid("User0001");
        return um;
    }
}


/**
 * 工作人员登录控制的逻辑处理
 */
 class WorkerLogin {
    /**
     * 判断登录数据是否正确，也就是是否能登录成功
     * @param lm 封装登录数据的Model
     * @return true表示登录成功，false表示登录失败
     */
    public boolean login(LoginModelWorker lm) {
        //1：根据工作人员编号去获取工作人员的数据
        WorkerModel wm = this.findWorkerByWorkerId(lm.getWorkerId());
        //2：判断从前台传递过来的用户名和加密后的密码数据，和数据库中已有的数据是否匹配
        if (wm != null) {
            //3：把从前台传来的密码数据，使用相应的加密算法进行加密运算
            String encryptPwd = this.encryptPwd(lm.getPwd());
            //如果工作人员存在，检查工作人员编号和密码是否匹配
            if (wm.getWorkerId().equals(lm.getWorkerId())
                    && wm.getPwd().equals(encryptPwd)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 对密码数据进行加密
     * @param pwd 密码数据
     * @return 加密后的密码数据
     */
    private String encryptPwd(String pwd){
        //这里对密码进行加密，省略了
        return pwd;
    }

    /**
     * 根据工作人员编号获取工作人员的详细信息
     * @param workerId 工作人员编号
     * @return 对应的工作人员的详细信息
     */
    private WorkerModel findWorkerByWorkerId(String workerId) {
        // 这里省略具体的处理，仅做示意，返回一个有默认数据的对象
        WorkerModel wm = new WorkerModel();
        wm.setWorkerId(workerId);
        wm.setName("Worker1");
        wm.setPwd("worker1");
        wm.setUuid("Worker0001");
        return wm;
    }
}



