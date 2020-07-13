import continuous_view


class PLPYPlanMock:
    def __init__(self, template, types):
        self.template = template
        self.types = types

    def fill(self, values):
        query = self.template
        counter = 1

        for value in values:
            if self.types[counter - 1] == 'text' or self.types[counter - 1] == 'name':
                value = '\'' + value + '\''
            query = query.replace('$' + str(counter), value, 1)
            counter += 1

        return query


class PLPYMock:
    def info(self, str):
        print('# INFO ##### ' + str + '\n')

    def prepare(self, template, types):
        return PLPYPlanMock(template, types)

    def execute(self, plan, values=None):
        if values is None:
            print(plan + '\n')
        else:
            print(plan.fill(values) + '\n')


if __name__ == '__main__':
    # test continuous views without PostgreSQL (just print all sql statements)
    continuous_view.create_continuous_view_from_file(PLPYMock(), 'query20',
                                                     'src/sql/tpch/queries/q20.sql')
