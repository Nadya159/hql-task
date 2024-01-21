package by.itacademy.hibernate.dao;


import by.itacademy.hibernate.convertor.BirthdayConvertor;
import by.itacademy.hibernate.dto.CompanyFilter;
import by.itacademy.hibernate.entity.Birthday;
import by.itacademy.hibernate.entity.Company;
import by.itacademy.hibernate.utils.TestDataImporter;
import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.User;
import by.itacademy.hibernate.util.HibernateUtil;
import com.querydsl.core.Tuple;
import lombok.Cleanup;
import org.assertj.core.api.ListAssert;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class UserDaoTest {

    private final SessionFactory sessionFactory = HibernateUtil.buildSessionFactory();
    private final UserDao userDao = UserDao.getInstance();

    @BeforeAll
    public void initDb() {
        TestDataImporter.importData(sessionFactory);
        sessionFactory.openSession();               //добавлено для открытия сессии выполнения тестов
    }

    @AfterAll
    public void finish() {
        sessionFactory.close();
    }

    @Test
    void findAll() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<User> results = userDao.findAll(session);
        System.out.println(results);                    //добавлено для проверки в консоли
        assertThat(results).hasSize(5);
        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).containsExactlyInAnyOrder("Bill Gates", "Steve Jobs", "Sergey Brin",
                "Tim Cook", "Diane Greene");
        session.getTransaction().commit();
    }

    @Test
    void findAllByFirstName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = userDao.findAllByFirstName(session, "Bill");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).fullName()).isEqualTo("Bill Gates");

        session.getTransaction().commit();
    }

    @Test
    void findLimitedUsersOrderedByBirthday() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        int limit = 3;
        List<User> results = userDao.findLimitedUsersOrderedByBirthday(session, limit);
        assertThat(results).hasSize(limit);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).contains("Diane Greene", "Steve Jobs", "Bill Gates");

        session.getTransaction().commit();
    }

    @Test
    void findAllByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = userDao.findAllByCompanyName(session, "Google");
        assertThat(results).hasSize(2);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).containsExactlyInAnyOrder("Sergey Brin", "Diane Greene");

        session.getTransaction().commit();
    }

    @Test
    void findAllPaymentsByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Payment> applePayments = userDao.findAllPaymentsByCompanyName(session, "Apple");
        assertThat(applePayments).hasSize(5);

        List<Integer> amounts = applePayments.stream().map(Payment::getAmount).collect(toList());
        assertThat(amounts).contains(250, 500, 600, 300, 400);

        session.getTransaction().commit();
    }

    @Test
    void findAveragePaymentAmountByFirstAndLastNames() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        Double averagePaymentAmount = userDao.findAveragePaymentAmountByFirstAndLastNames
                (session, "Bill", "Gates");
        assertThat(averagePaymentAmount).isEqualTo(300.0);

        session.getTransaction().commit();
    }

    @Test
    void findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Object[]> results = userDao.findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(session);
        assertThat(results).hasSize(3);

        List<String> orgNames = results.stream().map(a -> (String) a[0]).collect(toList());
        assertThat(orgNames).contains("Apple", "Google", "Microsoft");

        List<Double> orgAvgPayments = results.stream().map(a -> (Double) a[1]).collect(toList());
        assertThat(orgAvgPayments).contains(410.0, 400.0, 300.0);

        session.getTransaction().commit();
    }

    @Test
    void isItPossible() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Object[]> results = userDao.isItPossible(session);
        assertThat(results).hasSize(2);

        List<String> names = results.stream().map(r -> ((User) r[0]).fullName()).collect(toList());
        assertThat(names).contains("Sergey Brin", "Steve Jobs");

        List<Double> averagePayments = results.stream().map(r -> (Double) r[1]).collect(toList());
        assertThat(averagePayments).contains(500.0, 450.0);

        session.getTransaction().commit();
    }

    @Test
    void findMinAndMaxPaymentsOrderedByLastName(){
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> users = userDao.findMinAndMaxPaymentsOrderedByLastName(session);
        assertThat(users).hasSize(5);

        List<String> lastNames = users.stream().map(lastName -> lastName.get(0, String.class)).collect(toList());
        assertThat(lastNames).contains("Brin", "Cook", "Gates", "Greene", "Jobs");

        List<Integer> maxPayment = users.stream().map(maxPay -> maxPay.get(1, Integer.class)).collect(toList());
        assertThat(maxPayment).contains(500);

        List<Integer> minPayment = users.stream().map(minPay -> minPay.get(2, Integer.class)).collect(toList());
        assertThat(minPayment).contains(100);

        session.getTransaction().commit();
    }

    @Test
    void findMaxName(){
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        Integer maxName = userDao.findMaxName(session);
        assertThat(maxName).isEqualTo(11);
        session.getTransaction().commit();
    }

    @Test
    void findBirthdayByCompany(){
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        CompanyFilter filter = CompanyFilter.builder()
                .name("Google")
                .build();
        List<Birthday> birthdays = userDao.findBirthdayByCompany(session, filter);
        assertThat(birthdays).hasSize(2);

        LocalDate convertor = birthdays.get(0).birthDate();
        System.out.println("Convertor " + convertor);
        assertThat(convertor).isEqualTo(LocalDate.of(1973, 8, 21));
        session.getTransaction().commit();
    }

    @Test
    void findCountUserInCompany(){
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> countUsers = userDao.findCountUserInCompany(session);
        assertThat(countUsers).isNotEmpty();
        assertThat(countUsers).hasSize(3);

        List<Long> counts = countUsers.stream().map(it -> it.get(0, Long.class)).collect(toList());
        assertThat(counts).contains(2L, 2L, 1L);
        session.getTransaction().commit();
    }

    @Test
    void findSumPaymentInCompany(){
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> sumPayments = userDao.findSumPaymentInCompany(session);
        assertThat(sumPayments).isNotEmpty();
        assertThat(sumPayments).hasSize(3);

        List<Integer> sumPay = sumPayments.stream().map(it -> it.get(1, Integer.class)).collect(toList());
        assertThat(sumPay).contains(2050, 2400, 900);
        session.getTransaction().commit();
    }
}
