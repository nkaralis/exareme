// DATA_TEMPLATE: empty_table
oTest.fnStart("fnInitComplete");

/* Fairly boring function compared to the others! */

$(document).ready(function () {
    /* Check the default */
    var oTable = $('#example').dataTable({
        "bServerSide": true,
        "sAjaxSource": "../../../examples/server_side/scripts/server_processing.php"
    });
    var oSettings = oTable.fnSettings();
    var mPass;

    oTest.fnWaitTest(
        "Default should be null",
        null,
        function () {
            return oSettings.fnInitComplete == null;
        }
    );


    oTest.fnWaitTest(
        "Two arguments passed",
        function () {
            oSession.fnRestore();

            mPass = -1;
            $('#example').dataTable({
                "bServerSide": true,
                "sAjaxSource": "../../../examples/server_side/scripts/server_processing.php",
                "fnInitComplete": function () {
                    mPass = arguments.length === 2 && arguments[1] === undefined;
                }
            });
        },
        function () {
            return mPass;
        }
    );


    oTest.fnWaitTest(
        "That one argument is the settings object",
        function () {
            oSession.fnRestore();

            oTable = $('#example').dataTable({
                "bServerSide": true,
                "sAjaxSource": "../../../examples/server_side/scripts/server_processing.php",
                "fnInitComplete": function (oSettings) {
                    mPass = oSettings;
                }
            });
        },
        function () {
            return oTable.fnSettings() == mPass;
        }
    );


    oTest.fnWaitTest(
        "fnInitComplete called once on first draw",
        function () {
            oSession.fnRestore();

            mPass = 0;
            $('#example').dataTable({
                "bServerSide": true,
                "sAjaxSource": "../../../examples/server_side/scripts/server_processing.php",
                "fnInitComplete": function () {
                    mPass++;
                }
            });
        },
        function () {
            return mPass == 1;
        }
    );

    oTest.fnWaitTest(
        "fnInitComplete never called there after",
        function () {
            $('#example_next').click();
            $('#example_next').click();
            $('#example_next').click();
        },
        function () {
            return mPass == 1;
        }
    );


    oTest.fnWaitTest(
        "10 rows in the table on complete",
        function () {
            oSession.fnRestore();

            mPass = 0;
            $('#example').dataTable({
                "bServerSide": true,
                "sAjaxSource": "../../../examples/server_side/scripts/server_processing.php",
                "fnInitComplete": function () {
                    mPass = $('#example tbody tr').length;
                }
            });
        },
        function () {
            return mPass == 10;
        }
    );


    oTest.fnComplete();
});
