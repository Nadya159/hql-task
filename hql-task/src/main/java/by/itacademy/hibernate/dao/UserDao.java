package by.itacademy.hibernate.dao;

import by.itacademy.hibernate.dto.CompanyFilter;
import by.itacademy.hibernate.entity.*;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQuery;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hibernate.Session;

import static by.itacademy.hibernate.entity.QCompany.company;
import static by.itacademy.hibernate.entity.QPayment.payment;
import static by.itacademy.hibernate.entity.QPersonalInfo.personalInfo;
import static by.itacademy.hibernate.entity.QUser.user;

import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserDao {
    private static final UserDao INSTANCE = new UserDao();

    /**
     * Возвращает всех сотрудников
     */
    public List<User> findAll(Session session) {
        return session.createQuery("from User", User.class).list();
    }

    /**
     * Возвращает всех сотрудников с указанным именем
     */
    public List<User> findAllByFirstName(Session session, String firstName) {
        return session.createQuery("from User u where u.personalInfo.firstname = :firstName", User.class)
                .setParameter("firstName", firstName)
                .list();
    }

    /**
     * Возвращает первые {limit} сотрудников, упорядоченных по дате рождения (в порядке возрастания)
     */
    public List<User> findLimitedUsersOrderedByBirthday(Session session, int limit) {
        return session.createQuery("from User u order by personalInfo.birthDate", User.class)
                .setMaxResults(limit)
                .list();
    }

    /**
     * Возвращает всех сотрудников компании с указанным названием
     */
    public List<User> findAllByCompanyName(Session session, String companyName) {
        return session.createQuery("from User u where u.company.name = :companyName", User.class)
                .setParameter("companyName", companyName)
                .list();
    }

    /**
     * Возвращает все выплаты, полученные сотрудниками компании с указанными именем,
     * упорядоченные по имени сотрудника, а затем по размеру выплаты
     */
    public List<Payment> findAllPaymentsByCompanyName(Session session, String companyName) {
        return session.createQuery("from Payment p " +
                                   "where p.receiver.company.name = :companyName " +
                                   "order by p.receiver.username, p.amount", Payment.class)
                .setParameter("companyName", companyName)
                .list();
    }

    /**
     * Возвращает среднюю зарплату сотрудника с указанными именем и фамилией
     */
    public Double findAveragePaymentAmountByFirstAndLastNames(Session session, String firstName, String lastName) {
        return (double) session.createQuery(
                        "select avg(p.amount) from Payment p " +
                        "where p.receiver.personalInfo.firstname = :firstName " +
                        "and p.receiver.personalInfo.lastname = :lastName")
                .setParameter("firstName", firstName)
                .setParameter("lastName", lastName)
                .getSingleResult();
    }

    /**
     * Возвращает для каждой компании: название, среднюю зарплату всех её сотрудников. Компании упорядочены по названию.
     */
    public List<Object[]> findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(Session session) {
        return session.createQuery("select c.name, avg(p.amount) from Company c " +
                                   "join c.users u join u.payments p group by c.name order by c.name", Object[].class)
                .list();
    }

    /**
     * Возвращает список: сотрудник (объект User), средний размер выплат, но только для тех сотрудников,
     * чей средний размер выплат больше среднего размера выплат всех сотрудников
     * Упорядочить по имени сотрудника
     */
    public List<Object[]> isItPossible(Session session) {
        return session.createQuery("select u, avg(p.amount) from User u " +
                                   "join u.payments p " +
                                   "group by u.username " +
                                   "having avg(p.amount) > (select avg(p2.amount) from Payment p2)", Object[].class)
                .list();
    }

    /**
     * Максимальные и минимальные выплаты сотрудников, сортировка по фамилии
     */
    public List<Tuple> findMinAndMaxPaymentsOrderedByLastName(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user.personalInfo.lastname, payment.amount.max(), payment.amount.min())
                .from(payment)
                .join(payment.receiver, user)
                .groupBy(user.personalInfo.lastname)
                .fetch();
    }

    /**
     * Максимальная длина ФИО сотрудника
     */
    public Integer findMaxName(Session session) {
        return new JPAQuery<Integer>(session)
                .select((user.personalInfo.lastname.concat(user.personalInfo.firstname)).length().max())
                .from(user)
                .fetchOne();
    }

    /**
     * Даты рождения всех сотрудников заданной в фильтре компании
     */
    public List<Birthday> findBirthdayByCompany(Session session, CompanyFilter filter) {
        var predicate = QPredicate.builder()
                .add(filter.getName(), user.company.name::eq)
                .buildAnd();

        return new JPAQuery<List>(session)
                .select(personalInfo.birthDate)
                .from(user)
                .where(predicate)
                .fetch();
    }

    /**
     * Количество сотрудников в каждой компании
     */
    public List<Tuple> findCountUserInCompany(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user.count(), company.name)
                .from(user)
                .groupBy(company.name)
                .orderBy(company.name.asc())
                .fetch();
    }

    /**
     * Сумма выплат в каждой компании
     */
    public List<Tuple> findSumPaymentInCompany(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(company.name, payment.amount.sum())
                .from(company)
                .join(company.users, user)
                .join(user.payments, payment)
                .groupBy(company.name)
                .orderBy(company.name.asc())
                .fetch();
    }

    public static UserDao getInstance() {
        return INSTANCE;
    }
}