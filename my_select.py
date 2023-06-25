from sqlalchemy import func, desc, and_, distinct, select

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return:
    """

    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2():
    """
    Найти студента с наивысшим средним баллом по определенному предмету.
    :return:
    """

    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Student)\
        .join(Discipline)\
        .filter(Discipline.id == 3).group_by(Student.id).order_by(desc('avg_grade')).limit(1).all()
    return result


def select_3():
    """
    Найти средний балл в группах по определенному предмету.
    :return:
    """

    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Student)\
        .join(Group)\
        .join(Discipline)\
        .filter(Discipline.id == 3).group_by(Group.id).limit(4).all()
    return result


def select_4():
    """
    Найти средний балл на потоке (по всей таблице оценок).
    :return:
    """

    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade)\
        .join(Student)\
        .join(Group)\
        .join(Discipline)\
        .group_by(Group.id).limit(4).all()
    return result


def select_5():
    """
    Найти какие курсы читает определенный преподаватель.
    :return:
    """

    result = session.query(Discipline.name) \
        .select_from(Discipline)\
        .join(Teacher)\
        .filter(Teacher.id == 3).all()
    return result


def select_6():
    """
    Найти список студентов в определенной группе.

    """

    result = session.query(Student.fullname) \
        .select_from(Student)\
        .join(Group)\
        .filter(Group.id == 2).all()
    return result


def select_7():
    """
    Найти оценки студентов в отдельной группе по определенному предмету.
    :return:
    """

    result = session.query(Student.fullname, Grade.grade) \
        .select_from(Grade)\
        .join(Discipline)\
        .join(Student)\
        .join(Group)\
        .filter(and_(Discipline.id == 4, Group.id == 2)).all()
    return result


def select_8():
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return:
    """
    result = session.query(distinct(Teacher.fullname), func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline)  \
        .join(Teacher) \
        .where(Teacher.id == 1).group_by(Teacher.fullname).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_9():
    """
    Найти список курсов, которые посещает определенный студент.
    :return:
    """
    result = session.query(Discipline.name) \
        .select_from(Discipline) \
        .join(Grade) \
        .join(Student) \
        .filter(Student.id == 20).group_by(Discipline.name).all()
    return result


def select_10():
    """
    Список курсов, которые определенному студенту читает определенный преподаватель.

    """
    result = session.query(Discipline.name) \
        .select_from(Discipline) \
        .join(Teacher) \
        .join(Grade)    \
        .join(Student) \
        .filter(and_(Student.id == 25, Teacher.id == 1)).group_by(Discipline.name).all()
    return result


def select_11():
    """
    Средний балл, который определенный преподаватель ставит определенному студенту.

    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .join(Student) \
        .filter(and_(Student.id == 20, Teacher.id == 1)).group_by(Student.id).all()
    return result


def select_12():
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    :return:
    """
    group_id = 2
    dis_id = 2
    # Знаходимо останнє заняття
    subq = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == dis_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1)).scalar_subquery()

    result = session.query(Student.fullname, Discipline.name, Group.name, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(and_(Grade.discipline_id == dis_id, Group.id == group_id, Grade.date_of == subq)) \
        .order_by(desc(Grade.date_of)).all()
    return result


if __name__ == '__main__':
    print(select_1())
    print(select_2())
    print(select_3())
    print(select_4())
    print(select_5())
    print(select_6())
    print(select_7())
    print(select_8())
    print(select_9())
    print(select_10())
    print(select_11())
    print(select_12())
