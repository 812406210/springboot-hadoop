package com.hadoop.demo.entity;

import com.baomidou.mybatisplus.annotations.TableField;
import com.baomidou.mybatisplus.annotations.TableName;
import lombok.Data;
import java.io.Serializable;


@TableName(value = "user_t")
@Data
public class User implements Serializable {
    private Integer id;
    @TableField(value = "user_name")
    private String userName;

    private String password;

    private Integer age;
}
