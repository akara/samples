/*
 * TUserRequestnse (MIT)
 *
 * Copyright (c) 2013 Akara Sucharitakul
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.akara.samples.aggregator

import akka.actor.{ActorRef, Actor}
import java.util.Date

sealed trait LoginInfo
case class Login(userName: String) extends LoginInfo
case object NotLoggedIn extends LoginInfo

case class MemberInfoRequest(userName: String, login: LoginInfo)
case class UserByNameRequest(userName: String)
case class User(userName: String, userId: Long, summary: String)
case class ExtendedUserRequest(userId: Long)
case class ExtendedUserResponse(userId: Long, dob: Date, jobHistory: List[String], education: List[String])
case class ValidateCredentials(login: Login)
case class UserCredentials(userName: String, id: Long)

class MemberInfoActor extends Actor with Aggregator {

  expectOnce {
    case MemberInfoRequest(userName, login) =>
      fetchMemberInfo(sender, userName, login)
  }

  def fetchMemberInfo(requester: ActorRef, userName: String, login: LoginInfo) {

    val user = WriteOnce[User]

    // Get member overview
    context.actorSelection("/user/UserActor") ! UserByNameRequest(userName)
    expectOnce {
      case userResponse: User =>
        user := userResponse
    }

    // If user credentials match requested user, request more sensitive extended info.
    login match {
      case NotLoggedIn => user foreach { requester ! _ }
      case login: Login =>
        if (login.userName == userName) {
          checkCredentials(login)
        } else user foreach { requester ! _ }

    }

    // Validate the user credentials
    def checkCredentials(login: Login) {
      context.actorSelection("/user/CredentialValidator") ! ValidateCredentials(login)
      expectOnce {
        case UserCredentials(loginName, id) =>
          user foreach { userInfo =>
            if (userInfo.userId == id)
              requestExtendedUserInfo(id)
          }
      }
    }

    // Obtain extended (confidential) member info.
    def requestExtendedUserInfo(userId: Long) {
      context.actorSelection("/user/ExtendedUserActor") ! ExtendedUserRequest(userId)
      expectOnce {
        case extUser: ExtendedUserResponse =>
          requester ! (user, extUser)
      }
    }
  }
}