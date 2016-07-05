package servlet;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

import dao.DaoFactory;
import model.Section;
import service.ScheduleOfClassesService;
import service.dto.AjaxResult;
import service.dto.ListBoxDto;

/**
 * Servlet implementation class scheduleOfClassServlet
 */
@WebServlet("/scheduleOfClassServlet")
public class scheduleOfClassServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public scheduleOfClassServlet() {
        super();
        // TODO Auto-generated constructor stub
    }   
	
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		
		request.setCharacterEncoding("utf-8");
		response.setContentType("text/json");
		response.setCharacterEncoding("UTF-8");
		PrintWriter out = response.getWriter();
		
		//学期字段应该从前台request传入
		String semester = "SP2005";
		try {
			ScheduleOfClassesService scs = 
					new ScheduleOfClassesService(semester, DaoFactory.createSectioneDao());
			Map<String, Section> sections = scs.getScheduleOfClasses().getSectionsOffered();
			List<ListBoxDto> dtos = new ArrayList<ListBoxDto>();
			for(String key : sections.keySet()){
				dtos.add(
						new ListBoxDto(sections.get(key).getFullSectionNo()
								       , sections.get(key).getFullSectionInfo()));
			}			
			System.out.println(AjaxResult.success(dtos));
			out.print(AjaxResult.success(dtos));
			out.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();	
			out.print(AjaxResult.error("发生XX错误！"));
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);

	}

}
