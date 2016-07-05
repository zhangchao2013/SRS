package dao;

import java.util.HashMap;

import model.Professor;;

public interface PersonDao {
	
	public HashMap<String, Professor> findAllProfessors();

}
