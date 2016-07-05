<%@ page language="java" contentType="text/html; charset=utf-8"
    pageEncoding="utf-8" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>选课</title>
<link href="../script/ligerUI/skins/Aqua/css/ligerui-all.css" rel="stylesheet" />
    <script src="../script/jquery/jquery-1.7.1.min.js"></script>
    <script src="../script/ligerUI/js/ligerui.min.js"></script>

    <script src="../script/jquery-validation/jquery.validate.min.js"></script>
    <script src="../script/jquery-validation/jquery.metadata.js"></script>
    <script src="../script/jquery-validation/messages_cn.js"></script>
    <script src="../script/json2.js"></script>
    <style type="text/css">
        .middle input {
            display: block;
            width: 30px;
            margin: 2px;
        }
    </style>
</head>
<body>
    <form id="form1" class="liger-form" data-validate="{}">
        <div class="fields">
            <input data-type="text" data-label="StudentName" data-name="StudentName" validate="{required:true,minlength:5}" />
            <input data-type="text" data-label="ID Number" data-name="ID" validate="{required:true}" />
            <input data-type="text" data-label="Total Course" data-name="TotalCourse" validate="{required:false}" />
        </div>
        <div>
            <div style="margin: 4px; float: left;">
                <p>Schedule Of Classes</p>
                <div id="listbox1"></div>
            </div>
            <div style="margin: 4px; float: left;" class="middle">
                <p>&nbsp;&nbsp;</p>
                <input type="button" onclick="moveToRight()" value="->" />
                <input type="button" onclick="moveToLeft()" value="<-" />
            </div>
            <div style="margin: 4px; float: left;">
                <p>RegisteredFor</p>
                <div id="listbox2"></div>
            </div>
        </div>
    </form>
    <div class="liger-button" data-click="f_validate" data-width="150">Save</div>
</body>

<script type="text/javascript">
    $(function () {
        $("#listbox1,#listbox2").ligerListBox({
            isShowCheckBox: true,
            isMultiSelect: false,
            width: 450,
            height: 140
        });

        loadSchedule();
        loadResigistion();
    });
    //从服务器加载选课列表
    function loadSchedule() {
    	$.post(
                "../scheduleOfClassServlet",
                function (result) {
                	if(result.success){
                		liger.get("listbox1").setData(result.data);     
                	}                	                 	 
                }
            );         
    }
    //从服务器加载当前登陆学生已选课程
    function loadResigistion() {

    }
    function moveToLeft() {
        var box1 = liger.get("listbox1"), box2 = liger.get("listbox2");
        var selecteds = box2.getSelectedItems();
        if (!selecteds || !selecteds.length) return;
        box2.removeItems(selecteds);
        box1.addItems(selecteds);
    }
    function moveToRight() {
        var box1 = liger.get("listbox1"), box2 = liger.get("listbox2");
        var selecteds = box1.getSelectedItems();
        if (!selecteds || !selecteds.length) return;
        //调用后台执行选课操作
        $.post(
                "../enrollCourseServlet",
                function (result) {
                     //var json = JSON2.parse(result);
                     //如果选课成果才移动列表框项目，否则应弹出选课失败的原因
                     if(result.success){
                    	 box1.removeItems(selecteds);
                         box2.addItems(selecteds);
                     }else{
                    	 alert(result.message);
                     }                                    	 
                }
            );         
    }
    
    function f_validate(){
    	alert('点击了保存按钮');
    }
</script>
</html>
