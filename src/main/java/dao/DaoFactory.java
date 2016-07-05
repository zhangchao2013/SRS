package dao;

public class DaoFactory {
//	private static String daoName = "daoImpl.sqlite";
	private static String daoName = "daoImpl.mock";
	
	public static ScheduleDao createScheduleDao() {
		ScheduleDao result = null;
		try {
			Object o = Class.forName(daoName + "." + "ScheduleDao" + "Impl").newInstance();
			result = (ScheduleDao)o;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public static CourseDao createCourseDao() {
		CourseDao result = null;
		try {
			Object o = Class.forName(daoName + "." + "CourseDao" + "Impl").newInstance();
			result = (CourseDao)o;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public static SectionDao createSectioneDao() {
		SectionDao result = null;
		try {
			Object o = Class.forName(daoName + "." + "SectionDao" + "Impl").newInstance();
			result = (SectionDao)o;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}	
	
	public static UserDao createUserDao() {
		UserDao result = null;
		try {
			Object o = Class.forName(daoName + "." + "UserDao" + "Impl").newInstance();
			result = (UserDao)o;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
}
