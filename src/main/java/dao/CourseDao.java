package dao;

import java.util.HashMap;
import java.util.List;

import model.Course;
import model.CourseCatalog;

public interface CourseDao {
	
	public HashMap<String, Course> findAll();

}
