package com.test.Controller;

import com.test.entity.UserEntity;
import com.test.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@RestController
public class UserController {

    @Autowired
    private UserService userService;

    private static AtomicLong id = new AtomicLong(0);

    @GetMapping("/add_user")
    public String addUser(String username, String password){
        long idLong = id.incrementAndGet();
        userService.save(new UserEntity(idLong, username, password));
        UserEntity entity = userService.findById(idLong);
        return entity.toString();
    }

}
