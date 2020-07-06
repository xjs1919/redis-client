package com.test.service;

import com.test.dao.UserDao;
import com.test.entity.UserEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UserService {

    @Autowired
    private UserDao userDao;

    public UserEntity save(UserEntity userEntity){
        userDao.save(userEntity);
        return userEntity;
    }

    public UserEntity findById(Long id){
        Optional<UserEntity> optional = userDao.findById(id);
        return optional.isPresent()?optional.get():null;
    }

}
